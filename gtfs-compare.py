#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
import os, sys, contextlib, logging, pathlib, re, warnings, locale, enum
import collections, time, csv, datetime, pprint, textwrap, random

import pymysql, pymysql.cursors # https://pymysql.readthedocs.io/


class LogMessage(object):
	def __init__(self, fmt, a, k): self.fmt, self.a, self.k = fmt, a, k
	def __str__(self): return self.fmt.format(*self.a, **self.k) if self.a or self.k else self.fmt

class LogStyleAdapter(logging.LoggerAdapter):
	def __init__(self, logger, extra=None):
		super(LogStyleAdapter, self).__init__(logger, extra or {})
	def log(self, level, msg, *args, **kws):
		if not self.isEnabledFor(level): return
		log_kws = {} if 'exc_info' not in kws else dict(exc_info=kws.pop('exc_info'))
		msg, kws = self.process(msg, kws)
		self.logger._log(level, LogMessage(msg, args, kws), (), log_kws)

class LogStdoutHandler(logging.StreamHandler):
	def __init__(self): super().__init__(sys.stdout)
	def flush(self):
		self.acquire()
		try: self.stream.flush()
		except BrokenPipeError: pass
		finally: self.release()
	def close(self):
		self.acquire()
		try: self.stream.close()
		except BrokenPipeError: pass
		finally:
			super().close()
			self.release()

get_logger = lambda name: LogStyleAdapter(logging.getLogger(name))


def progress_iter(log, prefix, n_max, steps=30, n=0):
	'''Returns progress logging coroutine for long calculations.
		Use e.g. coro.send([result-size={:,}, res_size]) on each iteration.
		These messages will only be formatted and
			logged "steps" times, evenly spaced thru n_max iterations.'''
	steps, ts_start = min(n_max, steps), time.time()
	step_n = steps and n_max / steps
	msg_tpl = ( '[{{}}] Step {{:>{0}.0f}}'
		' / {{:{0}d}} {{:02,.0f}}.{{:02,.0f}}s{{}}' ).format(len(str(steps)))
	def _progress_iter_coro(n):
		while True:
			dn_msg = yield
			if isinstance(dn_msg, tuple): dn, msg = dn_msg
			elif isinstance(dn_msg, int): dn, msg = dn_msg, None
			else: dn, msg = 1, dn_msg
			n += dn
			if n == dn or n % step_n < 1:
				if msg:
					if not isinstance(msg, str): msg = msg[0].format(*msg[1:])
					msg = ': {}'.format(msg)
				ts_delta = time.time() - ts_start
				ts_delta_est = (n_max - n) / (n / ts_delta)
				log.debug(msg_tpl, prefix, n / step_n, steps, ts_delta, ts_delta_est, msg or '')
	coro = _progress_iter_coro(n)
	next(coro)
	return coro

def iter_range(a, b, step):
	if a > b: step = -step
	v = a
	while True:
		yield v
		if v == b: break
		v += step

it_adjacent = lambda seq, n, fill=None: it.zip_longest(fillvalue=fill, *([iter(seq)] * n))

class adict(dict):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.__dict__ = self
	def __setattr__(self, k, v):
		if k.startswith('_'): super().__setattr__(k, v)
		self[k] = v


class NTCursor(pymysql.cursors.SSDictCursor):
	'Returns namedtuple for each row.'

	@staticmethod
	@ft.lru_cache(maxsize=128)
	def tuple_for_row(row):
		if isinstance(row, str): return lambda *a: tuple(a)
		row = list(k.replace('.', ' ').rsplit(None, 1)[-1] for k in row)
		return collections.namedtuple('Row', row, rename=True)

	def __init__(self, *args, tuple_type=None, **kws):
		self._tt = tuple_type
		super().__init__(*args, **kws)

	def _conv_row(self, row):
		if row is None: return
		return ( self._tt(*row) if self._tt else
			self.tuple_for_row(tuple(self._fields))(*row) )

	def fetchall(self, bs=2**15):
		while True:
			rows = self.fetchmany(bs)
			if not rows: break
			yield from rows


@ft.total_ordering
class GTFSTimespan:

	weekday_order = 'monday tuesday wednesday thursday friday saturday sunday'.split()
	day = datetime.timedelta(days=1)

	def __init__(self, start, end, weekdays=None, except_days=None):
		assert all( isinstance(d, datetime.date)
			for d in it.chain([start, end], except_days or list()) ), [start, end, except_days]
		self.start, self.end = start, end
		if isinstance(weekdays, dict): weekdays = (weekdays[k] for k in self.weekday_order)
		self.weekdays, self.except_days = tuple(map(int, weekdays)), set(except_days or list())
		try: self.start, self.end = next(self.date_iter()), next(self.date_iter(reverse=True))
		except StopIteration: raise GTFSTimespanInvalid(str(self))
		self.except_days = frozenset(filter(
			lambda day: start <= day <= end and self.weekdays[day.weekday()], self.except_days ))
		self._hash_tuple = self.start, self.end, self.weekdays, self.except_days

	def __lt__(self, span): return self._hash_tuple < span._hash_tuple
	def __eq__(self, span): return self._hash_tuple == span._hash_tuple
	def __hash__(self): return hash(self._hash_tuple)

	def __repr__(self):
		weekdays = ''.join((str(n) if d else '.') for n,d in enumerate(self.weekdays, 1))
		except_days = ', '.join(map(str, self.except_days))
		return f'<TS {weekdays} [{self.start} {self.end}] {{{except_days}}}>'

	@property
	def weekday_dict(self): return dict(zip(self.weekday_order, self.weekdays))

	def date_iter(self, reverse=False):
		'Iterator for all valid (non-weekend/excluded) dates in this timespan.'
		return self.date_range( self.start, self.end,
			self.weekdays, self.except_days, reverse=reverse )

	@classmethod
	def date_range(cls, a, b, weekdays=None, except_days=None, reverse=False):
		if a > b: return
		if reverse: a, b = b, a
		svc_day_check = ( lambda day, wd=weekdays or [1]*7,
			ed=except_days or set(): wd[day.weekday()] and day not in ed )
		for day in filter(svc_day_check, iter_range(a, b, cls.day)): yield day


def dts_format(dts, sec=False):
	if dts is None: return '--:--'
	if isinstance(dts, str): return dts
	if isinstance(dts, datetime.timedelta): dts = dts.total_seconds()
	dts_days, dts = divmod(int(dts), 24 * 3600)
	dts = str(datetime.time(dts // 3600, (dts % 3600) // 60, dts % 60, dts % 1))
	if not sec: dts = dts.rsplit(':', 1)[0]
	if dts_days: dts = '{}+{}'.format(dts_days, dts)
	return dts

def stop_seq_str(seq):
	return ' - '.join(
		f'{stop}[{{}}]'.format(
			f'{dts_arr}/{dts_dep}' if dts_arr != dts_dep else dts_arr )
		for stop, dts_arr, dts_dep in seq )

GTFSEmbarkType = enum.IntEnum('EmbarkType', 'regular none phone driver', start=0)

def embark_format(t, v, reg_short=None):
	if reg_short and v == GTFSEmbarkType.regular: v = reg_short
	elif v == GTFSEmbarkType.none: v = '-'
	else: v = f'[{t}={p.name}]'
	return v

cif_train_status = {
	'B': 'bus', 'F': 'freight', 'P': 'passenger/parcels', 'S': 'ship', 'T': 'trip',
	'1': 'stp-passenger/parcels', '2': 'stp-freight',
		'3': 'stp-trip', '4':' stp-ship', '5': 'stp-bus' }
cif_train_category = {
	'OL': 'London Underground/Metro Service', 'OU': 'Unadvertised Ordinary Passenger',
	'OO': 'Ordinary Passenger', 'OS': 'Staff Train', 'OW': 'Mixed', 'XC': 'Channel Tunnel',
	'XD': 'Sleeper (Europe Night Services)', 'XI': 'International', 'XR': 'Motorail',
	'XU': 'Unadvertised Express', 'XX': 'Express Passenger', 'XZ': 'Sleeper (Domestic)',
	'BR': 'Bus – Replacement due to engineering work', 'BS': 'Bus – WTT Service', 'SS': 'Ship',
	'EE': 'Empty Coaching Stock (ECS)', 'EL': 'ECS, London Underground/Metro Service',
	'ES': 'ECS & Staff', 'JJ': 'Postal', 'PM': 'Post Office Controlled Parcels', 'PP': 'Parcels',
	'PV': 'Empty NPCCS', 'DD': 'Departmental', 'DH': 'Civil Engineer',
	'DI': 'Mechanical & Electrical Engineer', 'DQ': 'Stores', 'DT': 'Test',
	'DY': 'Signal & Telecommunications Engineer', 'ZB': 'Locomotive & Brake Van',
	'ZZ': 'Light Locomotive', 'J2': 'RfD Automotive (Components)', 'H2': 'RfD Automotive (Vehicles)',
	'J3': 'RfD Edible Products (UK Contracts)', 'J4': 'RfD Industrial Minerals (UK Contracts)',
	'J5': 'RfD Chemicals (UK Contracts)', 'J6': 'RfD Building Materials (UK Contracts)',
	'J8': 'RfD General Merchandise (UK Contracts)', 'H8': 'RfD European',
	'J9': 'RfD Freightliner (Contracts)', 'H9': 'RfD Freightliner (Other)',
	'A0': 'Coal (Distributive)', 'E0': 'Coal (Electricity) MGR', 'B0': 'Coal (Other) and Nuclear',
	'B1': 'Metals', 'B4': 'Aggregates', 'B5': 'Domestic and Industrial Waste',
	'B6': 'Building Materials (TLF)', 'B7': 'Petroleum Products',
	'H0': 'RfD European Channel Tunnel (Mixed Business)',
	'H1': 'RfD European Channel Tunnel Intermodal',
	'H3': 'RfD European Channel Tunnel Automotive',
	'H4': 'RfD European Channel Tunnel Contract Services',
	'H5': 'RfD European Channel Tunnel Haulmark', 'H6': 'RfD European Channel Tunnel Joint Venture' }
cif_operating_chars = {
	'B': 'Vacuum Braked', 'C': 'Timed at 100 m.p.h.', 'D': 'DOO (Coaching stock trains)',
	'E': 'Conveys Mark 4 Coaches', 'G': 'Trainman (Guard) required', 'M': 'Timed at 110 m.p.h.',
	'P': 'Push/Pull train', 'Q': 'Runs as required', 'R': 'Air conditioned with PA system',
	'S': 'Steam Heated', 'Y': 'Runs to Terminals/Yards as required',
	'Z': 'May convey traffic to SB1C gauge. Not to be diverted.' }
cif_power_type = {
	'D': 'Diesel/Steam', 'DEM': 'Diesel Electric Multiple Unit',
	'DMU': 'Diesel Mechanical Multiple Unit', 'E': 'Electric', 'ED': 'Electro-Diesel',
	'EML': 'EMU plus D, E, ED locomotive', 'EMU': 'Electric Multiple Unit',
	'HST': 'High Speed Train' }
cif_fields_maps = dict(
	train_status=cif_train_status, train_category=cif_train_category,
	operating_chars=cif_operating_chars, power_type=cif_power_type )


class GTFSDB:

	# Forces errors on truncated values and issues in multi-row inserts
	sql_mode = 'strict_all_tables'

	def __init__(self, conn_opts_base):
		self.conn_opts_base = conn_opts_base
		self.log, self.log_sql = get_logger('dtd2gtfs'), get_logger('dtd2gtfs.sql')

	def __enter__(self):
		self.db_conns, self.db_cursors = dict(), dict()
		self.ctx = contextlib.ExitStack()

		# Reset locale for consistency in calendar and such
		locale_prev = locale.setlocale(locale.LC_ALL, '')
		self.ctx.callback(locale.setlocale, locale.LC_ALL, locale_prev)

		# Warnings from pymysql about buffered results and such are all bugs
		self.ctx.enter_context(warnings.catch_warnings())
		warnings.filterwarnings('error')

		self.c = self.connect()
		self.db = self.db_conns[None]
		return self

	def __exit__(self, *exc):
		if self.ctx: self.ctx = self.ctx.close()
		self.db_conns = self.db_cursors = self.db = self.c = None


	def init_schema(self, db, schema_file_path, mem=False):
		self.log.debug( 'Initializing gtfs database'
			' (name={}, memory-engine={}) tables...', db, bool(mem) )
		with open(schema_file_path) as src: schema = src.read()
		if mem: schema = re.sub(r'(?i)\bENGINE=\S+\b', 'ENGINE=MEMORY', schema)
		# Not using "drop database if exists" here as it raises warnings
		self.c.execute( 'SELECT schema_name FROM'
			' information_schema.schemata WHERE schema_name=%s', db )
		if list(self.c.fetchall()): self.c.execute(f'drop database {db}')
		self.c.execute(f'create database {db}')
		self.c.execute(f'use {db}')
		self.c.execute(schema)
		self.commit()

	def connect(self, key=None):
		assert key not in self.db_conns, key
		conn = self.db_conns[key] = self.ctx.enter_context(
			contextlib.closing(pymysql.connect(charset='utf8mb4', **self.conn_opts_base)) )
		c = self.db_cursors[key] = self.ctx.enter_context(conn.cursor(NTCursor))
		c.execute('show variables like %s', ['sql_mode'])
		mode_flags = set(map(str.strip, dict(c.fetchall())['sql_mode'].lower().split(',')))
		mode_flags.update(self.sql_mode.lower().split())
		c.execute('set sql_mode = %s', [','.join(mode_flags)])
		return c

	def q(self, q, *params, fetch=True, c=None):
		# if self.log_sql.isEnabledFor(logging.DEBUG):
		# 	p_log = str(params)
		# 	if len(p_log) > 150: p_log = f'{p_log[:150]}...[len={len(p_log)}]'
		# 	self.log_sql.debug('{!r} {}', ' '.join(q.split()), p_log)
		c = self.c if not c else (self.db_cursors.get(c) or self.connect(c))
		c.execute(q, params)
		if not fetch: return c.lastrowid
		elif callable(fetch): return fetch(c)
		return c.fetchall()

	def qb(self, q, *params, c=None, fetch=True, **kws):
		'Query with buffered results.'
		res = self.q(q, *params, c=c or 'exec', fetch=fetch, **kws)
		return res if not fetch else list(res)

	def insert(self, db_or_table, table=None, **row):
		table = db_or_table if not table else f'{db_or_table}.{table}'
		row = collections.OrderedDict(row.items())
		cols, vals = ','.join(row.keys()), ','.join(['%s']*len(row))
		return self.qb(
			f'INSERT INTO {table} ({cols}) VALUES ({vals})',
			*row.values(), fetch=False )

	def commit(self):
		for conn in self.db_conns.values(): conn.commit()

	def escape(self, val):
		return self.db.escape(val)


	def iter_gtfs_tuples(self, gtfs_dir, filename, empty_if_missing=False, yield_fields=False):
		if filename.endswith('.txt'): filename = filename[:-4]
		tuple_t = ''.join(' '.join(filename.rstrip('s').split('_')).title().split())
		p = gtfs_dir / f'{filename}.txt'
		if empty_if_missing and not os.access(str(p), os.R_OK):
			if yield_fields: yield list()
			return
		with p.open(encoding='utf-8-sig') as src:
			src_csv = csv.reader(src)
			fields = list(v.strip() for v in next(src_csv))
			tuple_t = collections.namedtuple(tuple_t, fields)
			if yield_fields: yield fields
			for line in src_csv:
				try: yield tuple_t(*line)
				except TypeError: self.log.debug('Skipping bogus CSV line (file: {}): {!r}', p, line)

	def parse(self, db, gtfs_path, schema_file_path=None):
		if schema_file_path: self.init_schema(db, schema_file_path)
		gtfs_dir = pathlib.Path(gtfs_path)

		filter_csv_vals = lambda s: dict((k,v) for k,v in s._asdict().items() if v != '')
		gtfs_tables = 'trips calendar calendar_dates stops stop_times'.split()

		for n, table in enumerate(gtfs_tables, 1):
			with (gtfs_dir / f'{table}.txt').open() as src:
				count_lines = sum(bl.count('\n') for bl in iter(ft.partial(src.read, 2**20), '')) - 1
			count_rows, progress_steps = 0, max(5, min(30, int(count_lines / 3000)))
			self.log.debug( 'Processing gtfs file'
				' {!r} [{}/{}] ({:,} lines)...', table, n, len(gtfs_tables), count_lines )

			progress = progress_iter(self.log, table, count_lines, progress_steps)
			for s in self.iter_gtfs_tuples(gtfs_dir, table):
				progress.send(['rows={:,}', count_rows])
				row = filter_csv_vals(s)
				if table == 'stop_times' and not s.stop_id: continue
				self.insert(db, table, **row)
				count_rows += 1

			self.log.debug( 'Finished import for table {!r}:'
				' {:,} / {:,} row(s)', table, count_rows, count_lines )

		self.commit()


	def diff_check_skip_rollovers(self, diff):
		stop_sets = tuple(tuple(
			( tuple((crs, ts_arr[2:], ts_dep[2:]) for crs,ts_arr,ts_dep in stops)
				if stops[0][1].startswith('1+:') else stops )
			for stops in db_stops if stops ) for db_stops in stop_sets)
		diff_fixed = diff_func(*stop_sets)
		if diff_fixed: return diff

	def diff_check_skip_sched_times(self, diff):
		side1, side2 = (diff.get(f'set_item_{t}', list()) for t in ['added', 'removed'])
		if len(side1) != len(side2): return diff
		stops2_matched = set()
		for stops1 in map(op.attrgetter('t2'), side1):
			for stops2 in map(op.attrgetter('t1'), side2):
				if stops2 in stops2_matched or len(stops1) != len(stops2): continue
				for s1, s2, in zip(stops1, stops2):
					if s1 == s2: continue
					crs, ts_arr, ts_dep = s1
					if ts_arr == ts_dep and ts_arr in s2[1:]: continue
					crs, ts_arr, ts_dep = s2
					if ts_arr == ts_dep and ts_arr in s1[1:]: continue
					break
				else: # stops1 == stops2
					stops2_matched.add(stops2)
					break
			else: break # found non-matching stops1 - proper diff
		else: return # found match for all stops1, discard diff
		return diff

	def compare( self, db1, db2, cif_db=None, skip_diffs=None, train_uid_limit=None,
			train_uid_seek=None, train_uid_next=False, stop_after_train_uid_mismatch=False ):
		import deepdiff # http://deepdiff.readthedocs.io/
		log, dbs = self.log, (db1, db2)

		diff_func = ft.partial(deepdiff.DeepDiff, view='tree')
		diff_print = lambda diff: pprint.pprint(diff, indent=2) or True
		diff_print_fill = lambda text, tab1=' - ', tabn='   ': textwrap.fill(
			text, 100, initial_indent=tab1, subsequent_indent=tabn )

		skip_holidays = (skip_diffs and skip_diffs.get('holidays')) or set()
		skip_rollovers = skip_diffs and skip_diffs.get('rollover')
		skip_sched_times = skip_diffs and skip_diffs.get('sched_times')

		tuid_sets = tuple(
			set(map( op.itemgetter(0),
				self.q(f'SELECT DISTINCT(trip_headsign) FROM {db}.trips') ))
			for db in dbs )
		tuid_intersect = tuid_sets[0] & tuid_sets[1]
		for n,m in (0,1), (1,0):
			tuid_diff = tuid_sets[n].difference(tuid_sets[m])
			if not tuid_diff: continue
			log.info(
				'[{}] has trips for extra {:,} train_uid entries ({:,} same)',
				dbs[n], len(tuid_diff), len(tuid_intersect) )

		tuid_check = list(tuid_intersect)
		random.shuffle(tuid_check, lambda: 0.7401007202888102)
		tuid_check = tuid_check[:train_uid_limit or 2**30]
		log.debug('Comparing trips/stops for {} train_uids...', len(tuid_check))

		for train_uid in tuid_check:
			if train_uid_seek:
				if train_uid != train_uid_seek: continue
				train_uid_seek = None
				if train_uid_next: continue

			log.debug('Comparing data for train_uid={}...', train_uid)
			diff_found, stats = False, dict((db, collections.Counter()) for db in dbs)
			trip_info = dict((db, adict()) for db in dbs)

			### Populate trip_info
			for db in dbs:
				trip_span_idx, trip_stops_idx = collections.defaultdict(set), dict()
				trip_info[db].spans, trip_info[db].stops = trip_span_idx, trip_stops_idx
				trips = list(self.q(f'''
					SELECT
						t.trip_id,
						t.service_id AS svc_id,
						st.stop_id AS id,
						st.stop_sequence AS seq,
						st.arrival_time AS ts_arr,
						st.departure_time AS ts_dep
					FROM {db}.trips t
					LEFT JOIN {db}.stop_times st USING(trip_id)
					WHERE t.trip_headsign = %s
					ORDER BY t.trip_id, st.stop_sequence''', train_uid))
				for trip_id, stops in it.groupby(trips, op.attrgetter('trip_id')):
					trip, trip_stops = next(stops), list()
					if trip.id is None: stats[db]['trip-empty'] += 1
					else:
						for st in [trip, *stops]:
							if not (st.ts_arr or st.ts_dep):
								stats[db]['stop-no-times'] += 1
								continue
							ts_arr = dts_format(st.ts_arr or st.ts_dep)
							ts_dep = dts_format(st.ts_dep or st.ts_arr)
							trip_stops.append((st.id, ts_arr, ts_dep))
					trip_stops, trip_hash = tuple(trip_stops), hash(tuple(trip_stops))
					if trip.svc_id in trip_span_idx[trip_hash]: stats[db]['trip-dup'] += 1 # same stops/svc
					trip_span_idx[trip_hash].add(trip.svc_id)
					trip_stops_idx[trip_hash] = trip_stops

			log.debug('Comparing trip stops for train_uid={}...', train_uid)
			stop_sets = tuple(set(trip_info[db].stops.values()) for db in dbs)
			diff = diff_func(*stop_sets)
			if diff and skip_rollovers: diff = self.diff_check_skip_rollovers(diff)
			if diff and skip_sched_times: diff = self.diff_check_skip_sched_times(diff)
			if diff:
				diff_found |= True
				added, removed = diffs = \
					diff.get('set_item_added', list()), diff.get('set_item_removed', list())
				diff_seqs = set()
				log.info('Stop sequence(s) mismatch for train_uid: {}', train_uid)
				if len(added) == len(removed) == 1:
					print(f'--- [{train_uid}] Different stop sequence (t1={db1}, t2={db2}):')
					diff_print(diff_func(
						list(diff['set_item_removed'])[0].t1,
						list(diff['set_item_added'])[0].t2 ))
				elif bool(added) or bool(removed):
					for (k, t), seq_list in zip([(f'in {db2}', 't2'), (f'in {db1}', 't1')], diffs):
						if not seq_list: continue
						print(f'--- [{train_uid}] Only {k} ({len(seq_list)}):')
						for seq in seq_list:
							seq = getattr(seq, t)
							diff_seqs.add(seq)
							print(diff_print_fill(stop_seq_str(seq)))
				else:
					print('--- [{train_uid}] Multiple/mismatched changes:')
					diff_print(diff)
				print('all schedules in both dbs (for reference):')
				for db in dbs:
					print(f' - {db}:')
					for seq in sorted(set(trip_info[db].stops.values())):
						print(diff_print_fill(
							stop_seq_str(seq),
							' {} - '.format('x' if seq in diff_seqs else ' '), '     ' ))
				if cif_db: self.print_train_info(train_uid, cif_db, cif_comment=' (for reference)')

			log.debug('Comparing trip calendars for train_uid={}...', train_uid)
			th1, th2 = (set(trip_info[db].spans.keys()) for db in dbs)
			for trip_hash in th1 & th2:
				db_days, db_spans = list(), list()
				for db in dbs:
					svc_ids = ','.join(map(self.escape, trip_info[db].spans[trip_hash]))
					calendars = self.q(f'''
						SELECT
							service_id AS id, start_date AS a, end_date AS b,
							CONCAT(monday, tuesday, wednesday, thursday, friday, saturday, sunday) AS days,
							date, exception_type AS exc
						FROM {db}.calendar c
						LEFT JOIN {db}.calendar_dates cd USING(service_id)
						WHERE service_id IN ({svc_ids})
						ORDER BY service_id''')
					svc_days, svc_spans = set(), list()
					for svc_id, days in it.groupby(calendars, op.attrgetter('id')):
						svc = next(days)
						days = list() if svc.date is None else [svc, *days]
						exc_days = set(row.date for row in days if row.exc == 2)
						extra_days = set(row.date for row in days if row.exc == 1)
						span = GTFSTimespan(svc.a, svc.b, tuple(map(int, svc.days)), exc_days)
						svc_days.update(it.chain(span.date_iter(), extra_days))
						svc_spans.append((span, extra_days))
					db_days.append(set(svc_days))
					db_spans.append(svc_spans)
				days1, days2 = db_days
				diff = diff_func(days1, days2)
				diff_days = sorted(it.chain.from_iterable(
					map(op.attrgetter(k), days) for k,days in zip(['t2', 't1'], [
						diff.get('set_item_added', list()),
						diff.get('set_item_removed', list()) ]) if days ))
				if skip_holidays and skip_holidays.issuperset(diff_days): diff = None
				if diff:
					diff_found |= True
					log.info('Calendars mismatch for train_uid: {}', train_uid)
					print(f'--- [{train_uid}] Different service days (t1={db1}, t2={db2}):')
					seq = trip_info[db].stops[trip_hash]
					print('  trip stops:')
					print(diff_print_fill(stop_seq_str(seq), '    ', '    '))
					print(f'  service days: {db1}={len(days1):,} {db2}={len(days2):,}')
					print('  spans:')
					for db, spans in zip(dbs, db_spans):
						print(f'    {db}:')
						for span, extra_days in spans:
							extra_days = ( '' if not extra_days else
								'{{{}}}'.format(', '.join(map(str, sorted(extra_days)))) )
							print(f'      {span}{extra_days}')
					print(f'  diffs: {db1:^14}  {db2:^14}')
					print('         --------------  --------------')
					for day in sorted(it.chain.from_iterable(
							map(op.attrgetter(k), days) for k,days in zip(['t2', 't1'], [
								diff.get('set_item_added', list()),
								diff.get('set_item_removed', list()) ]) if days )):
						print('         {:^14}  {:^14}'.format(*(
							('{} [{}]'.format(day, day.weekday()+1) if day in days else '')
							for days in [days1, days2] )))
					if cif_db:
						self.print_train_info( train_uid, cif_db,
							prefix=2, cif_stops=False, cif_comment=' (for reference)' )

			for db in dbs:
				for k, v in stats[db].items(): log.info('[{}] Quirk count: {}={}', db, k, v)
			if diff_found and stop_after_train_uid_mismatch: break


	def print_train_info( self,
			train_uid=None, db_cif=None, db_gtfs=None,
			trip_id=None, prefix=None, cif_stops=True, cif_comment='' ):
		assert not (train_uid and trip_id), [train_uid, trip_id]
		pre = prefix or 0
		if isinstance(pre, int): pre *= ' '

		if db_cif and train_uid:
			train_assoc = train_uid.split('_')
			train_assoc, train_uid_tuple = len(train_assoc) > 1, ','.join(map(self.escape, train_assoc))
			z = 'z_' if train_uid.startswith('Z') else ''

			print(f'{pre}cif train/sched info{cif_comment}:')
			values = collections.defaultdict(lambda: collections.defaultdict(list))
			for sched_train_uid, schedules in it.groupby(self.q(f'''
					SELECT
						id, train_uid,
						train_status, train_category, train_identity, headcode, course_indicator,
						profit_center, business_sector, power_type, timing_load, speed,
						operating_chars, train_class, sleepers, reservations, connect_indicator,
						catering_code, service_branding
					FROM {db_cif}.{z}schedule s
					WHERE train_uid IN ({train_uid_tuple})
					ORDER BY train_uid'''), op.attrgetter('train_uid')):
				for s in schedules:
					for k, v in s._asdict().items():
						if k in ['id', 'train_uid']: continue
						if v:
							v_map = cif_fields_maps.get(k)
							if v_map:
								if k == 'operating_chars':
									v = '[{}]'.format(' // '.join(v_map.get(v, f'{v} [raw]') for v in v))
								else: v = v_map.get(str(v), f'{v} [raw]')
						values[k][v].append((sched_train_uid, s.id))
			for k, vals in values.items():
				if not set(vals.keys()).difference([None]): continue
				vals_print, train_count, sched_count = list(), set(), set()
				for v, sched_info in vals.items():
					trains, scheds = zip(*sched_info)
					train_count.update(trains)
					sched_count.update(scheds)
					vals_print.append((v, sched_info))
				for v, scheds in vals_print:
					sched_info = set()
					if len(vals_print) > 1:
						for t,s in scheds:
							if len(train_count) <= 1: t = None
							if len(sched_count) <= 1: s = None
							if t or s: sched_info.add('.'.join(map(str, filter(None, [t, s]))))
					sched_info = ' ({})'.format(', '.join(sorted(sched_info))) if sched_info else ''
					if v is not None: print(f'{pre}  {k}: {v}{sched_info}')

			print(f'{pre}cif {z}schedules{"/stops" if cif_stops else ""}{cif_comment}:')
			sched_query = f'''
				SELECT
					s.train_uid, s.id, stp_indicator AS stp,
					runs_from AS a , runs_to AS b, bank_holiday_running AS always,
					CONCAT(monday, tuesday, wednesday, thursday, friday, saturday, sunday) AS days
					{'-- , crs_code' if not z else ', location AS crs_code'}
					-- , public_arrival_time AS ts_arr, public_departure_time AS ts_dep, activity
				FROM {db_cif}.{z}schedule s
				-- LEFT JOIN {db_cif}.{z}stop_time st ON st.{z}schedule = s.id
				{f'-- LEFT JOIN {db_cif}.tiploc t ON t.tiploc_code = st.location' if not z else ''}
				WHERE
					train_uid IN ({train_uid_tuple})
					{'-- AND (st.id IS NULL OR t.crs_code IS NOT NULL)' if not z else ''}
				ORDER BY
					s.train_uid, -- FIELD(stp_indicator,'P','O','N','C'),
					s.id -- , st.id'''
			sched_query = re.sub( r'[\t ]*-- (.*(\n|$))',
				r'\1' if cif_stops else '', sched_query )
			sched_query = re.sub('(?<=\S)\t+(?=\S)', ' ', sched_query).replace('\t', '  ')
			for s in self.q(sched_query):
				days = ''.join(str(n if d else '.') for n,d in zip(range(1, 8), map(int, s.days)))
				if cif_stops:
					activity = s.activity or ''
					if len(activity) % 2: activity += ' '
					activity = '/'.join(filter( None,
						(activity[n:n+2].strip() for n in range(0, len(activity), 2)) ))
					print(
						f'{pre}  {s.train_uid} {s.id:>7d} {s.stp} {s.a} {s.b} {days}',
						'A' if s.always else ' ', s.crs_code or '---',
						dts_format(s.ts_arr), dts_format(s.ts_dep), activity )
				else:
					print(
						f'{pre}  {s.train_uid} {s.id:>7d} {s.stp} {s.a} {s.b} {days} ',
						'[no-holidays]' if s.always else '' )

			assocs = list(self.q(f'''
				SELECT
					a.id, base_uid, assoc_uid, start_date AS a, end_date AS b,
					stp_indicator as stp, crs_code, assoc_cat, assoc_date_ind,
					CONCAT(monday, tuesday, wednesday, thursday, friday, saturday, sunday) AS days
				FROM {db_cif}.association a
				JOIN {db_cif}.tiploc tl ON a.assoc_location = tl.tiploc_code
				WHERE a.base_uid IN ({train_uid_tuple}) OR a.assoc_uid IN ({train_uid_tuple})
				ORDER BY a.base_uid, a.assoc_uid, FIELD(a.stp_indicator,'P','O','N','C'), a.id'''))
			if assocs or train_assoc:
				print(f'{pre}cif associations{cif_comment}:')
				for a in assocs:
					days = ''.join(str(n if d else '.') for n,d in zip(range(1, 8), map(int, a.days)))
					print(
						( f'{pre}  {a.base_uid} {a.assoc_uid} {a.id:>7d}'
							f' {a.stp} {a.assoc_cat or "--"} {{}} {a.crs_code} {a.a} {a.b} {days}' )\
						.format('N' if a.assoc_date_ind == 'N' else '-') )

		if db_gtfs:
			print(f'{pre}gtfs trips:')
			db_trips = list(self.q(f'''
				SELECT
					t.trip_id,
					t.service_id AS svc_id,
					t.trip_headsign AS train_uid,
					st.stop_id AS id,
					st.stop_sequence AS seq,
					st.arrival_time AS ts_arr,
					st.departure_time AS ts_dep,
					st.pickup_type AS pickup,
					st.drop_off_type AS dropoff
				FROM {db_gtfs}.trips t
				LEFT JOIN {db_gtfs}.stop_times st USING(trip_id)
				WHERE {"t.trip_headsign = %s" if train_uid else "t.trip_id = %s"}
				ORDER BY t.trip_id, st.stop_sequence''', train_uid or trip_id))
			for trip_id, stops in it.groupby(db_trips, op.attrgetter('trip_id')):
				trip, trip_stops = next(stops), list()
				if trip.id is not None:
					for st in [trip, *stops]:
						if st.ts_arr: ts_arr = dts_format(st.ts_arr or st.ts_dep)
						if st.ts_dep: ts_dep = dts_format(st.ts_dep or st.ts_arr)
						trip_stops.append(( st.id, ts_arr, ts_dep,
							*map(GTFSEmbarkType, [st.pickup, st.dropoff]) ))
				calendars = self.q(f'''
					SELECT
						service_id AS id, start_date AS a, end_date AS b,
						CONCAT(monday, tuesday, wednesday, thursday, friday, saturday, sunday) AS days,
						date, exception_type AS exc
					FROM {db_gtfs}.calendar c
					LEFT JOIN {db_gtfs}.calendar_dates cd USING(service_id)
					WHERE service_id = %s''', trip.svc_id)
				svc_days = set()
				for svc_id, days in it.groupby(calendars, op.attrgetter('id')):
					svc = next(days)
					days = list() if svc.date is None else [svc, *days]
					exc_days = set(row.date for row in days if row.exc == 2)
					extra_days = set(row.date for row in days if row.exc == 1)
					span = GTFSTimespan(svc.a, svc.b, tuple(map(int, svc.days)), exc_days)
					svc_days.update(it.chain(span.date_iter(), extra_days))
				print( f'{pre}  [{trip_id}] headsign={trip.train_uid}'
					f' svc={trip.svc_id} svc_days={len(svc_days)}:' )
				print(f'{pre}    {span}')
				if extra_days: print(f'{pre}    extra days:', ',  '.join(map(str, extra_days)))
				print(f'{pre}    stop sequence:')
				for stop_id, ts_arr, ts_dep, pickup, dropoff in trip_stops:
					p, d = embark_format('pickup', pickup, 'P'), embark_format('dropoff', dropoff, 'D')
					print(f'{pre}      {stop_id} {ts_arr or "-":^3s} {ts_dep or "-":^3s} {p}{d}')


	def print_stop_info(self, crs, db_cif):
		print(f'Station: {crs}')
		seen = dict()
		for s in self.q(f'''
				SELECT *
				FROM {db_cif}.physical_station s
				LEFT JOIN {db_cif}.tiploc tl USING(tiploc_code)
				LEFT JOIN {db_cif}.alias a USING(station_name)
				WHERE s.crs_code = %s''', crs):
			for k, v in s._asdict().items():
				if k == 'id' or k.startswith('_'): continue
				if seen.get(k, ...) == v: continue
				seen[k] = v
				print(f'  {k}: {v}')


def main(args=None):
	import argparse
	parser = argparse.ArgumentParser(
		description='Tool to compare gtfs feeds.')

	group = parser.add_argument_group('MySQL db parameters')
	group.add_argument('-f', '--mycnf-file',
		metavar='path', default=str(pathlib.Path('~/.my.cnf').expanduser()),
		help='Alternative ~/.my.cnf file to use to read all connection parameters from.'
			' Parameters there can include: host, port, user, passwd, connect,_timeout.'
			' Overidden parameters:'
				' db (specified via --src-cif-db/--dst-gtfs-db options),'
				' charset=utf8mb4 (for max compatibility).')
	group.add_argument('-g', '--mycnf-group', metavar='group',
		help='Name of "[group]" (ini section) in ~/.my.cnf ini file to use parameters from.')

	group = parser.add_argument_group('Misc other options')
	group.add_argument('--debug', action='store_true', help='Verbose operation mode.')

	cmds = parser.add_subparsers(title='Commands', dest='call')


	cmd = cmds.add_parser('import', help='Import gtfs feed from txt files into mysql db.')
	cmd.add_argument('src_path', help='Path with gtfs txt files to import.')
	cmd.add_argument('db_name', help='Destination mysql database to use.')
	cmd.add_argument('-i', '--gtfs-schema',
		metavar='path-to-schema.sql', nargs='?', const='doc/db-schema-gtfs.sql',
		help='Create/init destination database with schema from specified .sql file.'
			' If such database already exists, it will be dropped first!'
			' Default schema file path (if not specified as optional argument): %(default)s.')


	cmd = cmds.add_parser('compare', help='Compare data between two mysql dbs.')

	group = cmd.add_argument_group('Database options')
	group.add_argument('db1', help='Database-1 to compare Database-2 against.')
	group.add_argument('db2', help='Database-2 to compare Database-1 against.')
	group.add_argument('-c', '--cif-db', metavar='db-name',
		help='Name of CIF database to pull/display reference data from on mismatches.')

	group = cmd.add_argument_group('Subset selection')
	group.add_argument('-u', '--train-uid-seek', metavar='train_uid',
		help='Skip to diff of specified train_uid (ignoring all diffs before it).')
	group.add_argument('-x', '--train-uid-seek-next', action='store_true',
		help='When using -s/--train-uid-seek, skip to diff for train_uid right after specified one.')
	group.add_argument('-n', '--train-uid-limit', metavar='n', type=int,
		help='Stop after comparing data for specified number of train_uids.')
	group.add_argument('-1', '--stop-after-train-uid-mismatch',
		action='store_true', help='Stop after encountering first mismatch for train_uid data.')

	group = cmd.add_argument_group('Diff class skipping')
	cmd.add_argument('--skip-midnight-rollover-diffs', action='store_true',
		help='Skip diffs where stop times start at 00:xx and have +1 day in one case.')
	cmd.add_argument('--skip-arr-dep-time-diffs', action='store_true',
		help='Skip diffs where there is extra arr/dep time, missing in other dataset.'
			' Workaround for ts implementation using scheduled times for public stops.')
	cmd.add_argument('--skip-bank-holiday-diffs', metavar='holiday-file',
		help='Skip diffs that only have bank-holiday dates from specified list file.')
	cmd.add_argument('--skip-bank-holiday-fmt',
		metavar='strptime-format', default='%d-%b-%Y',
		help='strptime() format for each line in --skip-bank-holiday-diffs file. Default: %(default)s')


	cmd = cmds.add_parser('query',
		help='Show info from gtfs/cif databases for specific train/trip/stop.')
	cmd.add_argument('-c', '--db-cif', metavar='db-name', help='CIF database name.')
	cmd.add_argument('-d', '--db-gtfs', metavar='db-name', help='GTFS database name.')
	cmd.add_argument('-u', '--train-uid', metavar='uid',
		help='Single train_uid to query/show information for.')
	cmd.add_argument('-t', '--trip-id', metavar='id',
		help='GTFS trip_id value to print info for.')
	cmd.add_argument('-s', '--stop-crs', metavar='crs-code',
		help='Show info for specified 3-letter stop CRS code from CIF data.')


	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	# Force line buffering for stdout, instead of default chunks when it's not tty
	sys.stdout = open(sys.stdout.fileno(), 'w', 1)
	logging.basicConfig( handlers=[LogStdoutHandler()],
		level=logging.DEBUG if opts.debug else logging.INFO,
		format='%(asctime)s :: %(name)s %(levelname)s :: %(message)s' )
	log = get_logger('main')

	mysql_conn_opts = dict(filter(op.itemgetter(1), dict(
		read_default_file=opts.mycnf_file, read_default_group=opts.mycnf_group ).items()))
	with GTFSDB(mysql_conn_opts) as db:

		if opts.call == 'import':
			db.parse(opts.db_name, opts.src_path, opts.gtfs_schema)

		elif opts.call == 'compare':
			skip = dict( holidays=set(),
				rollover=opts.skip_midnight_rollover_diffs,
				sched_times=opts.skip_arr_dep_time_diffs )
			if opts.skip_bank_holiday_diffs:
				with pathlib.Path(opts.skip_bank_holiday_diffs).open() as src:
					for line in src.read().splitlines(): skip['holidays'].add(
						datetime.datetime.strptime(line, opts.skip_bank_holiday_fmt).date() )
			db.compare(
				opts.db1, opts.db2, cif_db=opts.cif_db, train_uid_limit=opts.train_uid_limit,
				train_uid_seek=opts.train_uid_seek, train_uid_next=opts.train_uid_seek_next,
				stop_after_train_uid_mismatch=opts.stop_after_train_uid_mismatch, skip_diffs=skip )

		elif opts.call == 'query':
			if opts.train_uid:
				if not (opts.db_cif or opts.db_gtfs):
					parser.error('Either --db-cif or --db-gtfs must be specified for --train-uid info.')
				db.print_train_info(opts.train_uid, opts.db_cif, opts.db_gtfs)
			if opts.trip_id:
				if not opts.db_gtfs:
					parser.error('--db-gtfs must be specified for --trip-id info.')
				db.print_train_info(trip_id=opts.trip_id, db_gtfs=opts.db_gtfs)
			if opts.stop_crs:
				db.print_stop_info(opts.stop_crs, opts.db_cif)

		else: parser.error(f'Action not implemented: {opts.call}')

if __name__ == '__main__': sys.exit(main())
