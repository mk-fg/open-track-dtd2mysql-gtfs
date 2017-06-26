#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
import os, sys, contextlib, logging, pathlib, re
import collections, secrets, enum, math, time
import datetime, calendar, locale

import MySQLdb, MySQLdb.cursors # https://mysqlclient.readthedocs.io/


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

def log_lines(log_func, lines, log_func_last=False):
	if isinstance(lines, str):
		lines = list(line.rstrip() for line in lines.rstrip().split('\n'))
	uid = secrets.token_urlsafe(3)
	for n, line in enumerate(lines, 1):
		if isinstance(line, str): line = '[{}] {}', uid, line
		else: line = ['[{}] {}'.format(uid, line[0])] + list(line[1:])
		if log_func_last and n == len(lines): log_func_last(*line)
		else: log_func(*line)

get_logger = lambda name: LogStyleAdapter(logging.getLogger(name))

def progress_iter(log, prefix, n_max, steps=30, n=0):
	'''Returns progress logging coroutine for long calculations.
		Use e.g. coro.send([result-size={:,}, res_size]) on each iteration.
		These messages will only be formatted and
			logged "steps" times, evenly spaced thru n_max iterations.'''
	steps = min(n_max, steps)
	step_n = steps and n_max / steps
	msg_tpl = '[{{}}] Step {{:>{0}.0f}} / {{:{0}d}}{{}}'.format(len(str(steps)))
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
				log.debug(msg_tpl, prefix, n / step_n, steps, msg or '')
	coro = _progress_iter_coro(n)
	next(coro)
	return coro

it_ngrams = lambda seq, n: zip(*(it.islice(seq, i, None) for i in range(n)))


class NTCursor(MySQLdb.cursors.Cursor):
	'Returns namedtuple for each row.'

	@staticmethod
	@ft.lru_cache(maxsize=128)
	def tuple_for_row(row):
		if isinstance(row, str): return lambda *a: tuple(a)
		row = list(k.replace('.', ' ').rsplit(None, 1)[-1] for k in row)
		return collections.namedtuple(f'Row', row, rename=True)

	@classmethod
	def with_keys(cls, row):
		return ft.partial(cls, tuple_type=cls.tuple_for_row(row))

	def __init__(self, *args, tuple_type=None, **kws):
		self._tt = tuple_type
		self._fetch_type = 1 if self._tt else 2
		super().__init__(*args, **kws)

	def _row(self, row):
		if row is None: return
		return ( self._tt(*row) if self._tt else
			self.tuple_for_row(tuple(row.keys()))(*row.values()) )

	def fetchone(self):
		return self._row(super().fetchone())
	def fetchmany(self, size=None):
		for row in super().fetchmany(size=size): yield self._row(row)
	def fetchall(self):
		for row in super().fetchall(): yield self._row(row)


class ConversionError(Exception): pass
class CIFError(ConversionError): pass


GTFSRouteType = enum.IntEnum( 'RouteType',
	'light_rail subway rail bus ferry cable_car gondola funicular spacecraft', start=0 )
GTFSPickupType = enum.IntEnum('PickupType', 'regular none phone driver', start=0)
GTFSExceptionType = enum.IntEnum('ExceptionType', 'added removed')


@ft.total_ordering
class GTFSTimespan:

	weekday_order = 'monday tuesday wednesday thursday friday saturday sunday'.split()

	def __init__(self, start, end, except_days=None, weekdays=None):
		assert all(isinstance(d, datetime.date) for d in it.chain([start, end], except_days or list()))
		self.start, self.end = start, end
		if isinstance(weekdays, dict): weekdays = tuple(weekdays[k] for k in self.weekday_order)
		self.weekdays = tuple(weekdays)
		self.except_days = frozenset(filter(
			lambda day: not (start <= day <= end and self.weekdays[day.weekday()]),
			except_days or list() )) # filters-out redundant exceptions that weren't valid anyway
		self._hash_tuple = self.start, self.end, self.weekdays, self.except_days

	def __lt__(self, span): return self._hash_tuple < span._hash_tuple
	def __eq__(self, span): return self._hash_tuple == span._hash_tuple
	def __hash__(self): return hash(self._hash_tuple)

	@property
	def weekday_dict(self): return dict(zip(self.weekday_order, self.weekdays))

	def merge(self, span, exc_days_to_split=5):
		'''Returns merged timespan or None if it's not possible.
			Simple algo here only merges overlapping or close intervals with same weekdays.
			exc_days_to_split = number of days in sequence
				to tolerate as exceptions when merging two spans with small gap in-between.'''
		if self.weekdays != span.weekdays: return
		s1, s2, weekdays = self, span, self.weekdays
		if s1 == s2: return s1
		if s1 > s2: s1, s2 = s2, s1
		if s1.end <= s2.start: # overlap
			return GTFSTimespan(
				s1.start, max(s1.end, s2.end),
				s1.except_days | s2.except_days, weekdays )
		if math.ceil((s2.start - s1.end) * (sum(weekdays) / 7)) <= exc_days_to_split:
			day, except_days = s1.end, set(s1.except_days | s2.except_days)
			while day < s2.start:
				day += datetime.timedelta(days=1)
				if weekdays[day.weekday()]: except_days.add(day)
			return GTFSTimespan(s1.start, s2.end, except_days, weekdays)


class DTDtoGTFS:

	# Forces errors on truncated values and issues in multi-row inserts
	sql_mode = 'strict_all_tables'

	def __init__(self, db_cif, db_gtfs, conn_opts_base, bank_holidays):
		self.db_cif, self.db_gtfs = db_cif, db_gtfs
		self.conn_opts_base, self.bank_holidays = conn_opts_base, bank_holidays
		self.log, self.log_sql = get_logger('dtd2gtfs'), get_logger('dtd2gtfs.sql')

	def __enter__(self):
		self.ctx = contextlib.ExitStack()

		# Reset locale for consistency in calendar and such
		locale_prev = locale.setlocale(locale.LC_ALL, '')
		self.ctx.callback(locale.setlocale, locale.LC_ALL, locale_prev)

		self.db = self.ctx.enter_context(
			contextlib.closing(MySQLdb.connect(charset='utf8mb4', **self.conn_opts_base)) )
		with self.db.cursor() as cur:
			cur.execute('show variables like %s', ['sql_mode'])
			mode_flags = set(map(str.strip, dict(cur.fetchall())['sql_mode'].lower().split(',')))
			mode_flags.update(self.sql_mode.lower().split())
			cur.execute('set sql_mode = %s', [','.join(mode_flags)])
		return self

	def __exit__(self, *exc):
		if self.ctx: self.ctx = self.ctx.close()


	## Conversion routine

	def q(self, q, *params, fetch=True):
		with self.db.cursor(NTCursor) as cur:
			# if self.log_sql.isEnabledFor(logging.DEBUG):
			# 	p_log = str(params)
			# 	if len(p_log) > 150: p_log = f'{p_log[:150]}...[len={len(p_log)}]'
			# 	self.log_sql.debug('{!r} {}', ' '.join(q.split()), p_log)
			cur.execute(q, params)
			if not fetch: return cur.lastrowid
			elif callable(fetch): return fetch(cur)
			return cur.fetchall()

	def insert(self, table, **row):
		row = collections.OrderedDict(row.items())
		cols, vals = ','.join(row.keys()), ','.join(['%s']*len(row))
		return self.q( f'INSERT INTO'
			f' {table} ({cols}) VALUES ({vals})', *row.values(), fetch=False )

	def run(self, test_run_slice=None):
		q, insert = self.q, self.insert


		### Stops
		# XXX: check if there are locations without crs code and what that means
		# XXX: convert physical_station eastings/northings to long/lat
		q('''
			INSERT INTO gtfs.stops
				( stop_id, stop_code, stop_name, stop_desc,
					stop_timezone, location_type, wheelchair_boarding )
				SELECT
					crs_code, tiploc_code, description, description,
						"Europe/London", 0, 0
				FROM cif.tiploc
				WHERE
					crs_code IS NOT NULL
					AND description IS NOT NULL''')


		### Transfers
		# XXX: better data source / convert to trip+frequences?
		# XXX: losing the mode here, TUBE, WALK, etc
		q('''
			INSERT INTO gtfs.transfers
				SELECT null, origin, destination, 2, duration * 60
				FROM cif.fixed_link''')
		# Interchange times
		q('''
			INSERT INTO gtfs.transfers
				SELECT null, crs_code, crs_code, 2, minimum_change_time * 60
				FROM cif.physical_station''')


		### Process cif.schedule entries to gtfs.trips/gtfs.calendar/gtfs.calendar_dates
		#
		# Due to many-to-one relation between gtfs.trips and gtfs.calendar
		#   (through service_id field), this is done in 3 sequential steps:
		#  - populate gtfs.trips/gtfs.stop_times
		#  - merge calendar timespans for trips where possible
		#  - populate gtfs.calendar/gtfs.calendar_dates, cloning trips/stops where necessary
		#
		# So essentially converting many-to-many relation into many-to-one
		#  with redundant trips, using trip_svc_timespans as a temporary mtm index.
		#
		# XXX: processing of Z schedules
		# XXX: processing of associations

		trip_svc_timespans = collections.defaultdict(list) # {trip_id: timespans}
		stats, stats_by_train = collections.Counter(), collections.defaultdict(collections.Counter)


		## Step-1: Add trips and stop_times for schedules
		#
		# - Only one gtfs.trip_id is created for same
		#   trip_hash (train_uid+stops+stop_times) via trip_merge_idx.
		#
		# - No calendar/service_id entries are created here -
		#   these will be merged in Step-2 via trip_svc_timespans,
		#   and gtfs.trips will be updated with service_id values in Step-3,
		#   duplicating gtfs.trip/gtfs.stop_times for timespans that can't be merged.

		route_ids = dict() # {(src, dst}: id}
		trip_merge_idx = dict() # {trip_hash: gtfs.trip_id}

		schedules = f'''
			SELECT *
			FROM cif.schedule s

			--[ Random selection used with test_run_slice only ]--
			JOIN
				( SELECT DISTINCT(train_uid) AS train_uid
					FROM cif.schedule rs
					JOIN
						(SELECT CEIL(RAND() * (SELECT MAX(id) FROM cif.schedule)) AS id) rss
						ON rs.id >= rss.id
					GROUP BY train_uid HAVING COUNT(*) > 0
					LIMIT {test_run_slice} ) r
				ON r.train_uid = s.train_uid
			--[ end ]--

			LEFT JOIN cif.schedule_extra e ON e.schedule = s.id
			ORDER BY s.train_uid, s.id'''
		schedules = re.sub(r'(?s)--\[.*{}\]--'.format('?' if test_run_slice else ''), '', schedules)
		sched_count, schedules = q(schedules, fetch=lambda c: (c.rowcount, c.fetchall()))
		ts_start, progress = time.time(), progress_iter(self.log, 'schedules', sched_count)
		stats['sched-count'] = sched_count

		self.log.debug('Processing {} cif.schedule entries...', sched_count)
		for n, s in enumerate(schedules):
			ts_delta = time.time() - ts_start
			ts_delta_est = ((sched_count - n) / (n / ts_delta)) if n else 0
			progress.send([
				'{1:02,.0f}.{2:02,.0f}s trips={0:,}',
				len(trip_svc_timespans), ts_delta, ts_delta_est ])
			stats[f'sched-entry-{s.stp_indicator}'] += 1
			stats_by_train[s.train_uid][f'stp-{s.stp_indicator}'] += 1

			# XXX: handle overrides later
			if s.stp_indicator != 'P': continue

			## Get stop sequence information and create gtfs.route_id, ignoring stop times for now
			#
			#  - cif.train_uid does not uniquely identify sequence of gtfs.stops (places),
			#    even for stp_indicator=P schedules only, for example train_uid=W34606
			#  - cif.schedule specifies both stop sequence and their days/times

			stops = list(q('''
				SELECT * FROM cif.stop_time st
				JOIN cif.tiploc t ON t.tiploc_code = st.location
				JOIN gtfs.stops s ON t.crs_code = s.stop_id
				WHERE schedule = %s
				ORDER BY
					public_arrival_time, scheduled_arrival_time,
					public_departure_time, scheduled_departure_time''', s.id))
			if not stops:
				# XXX: there are quite a lot of these - check what these represent
				# self.log.info('Skipping schedule with no usable stops: {}', s)
				continue

			# XXX: note on route_ids? - "how to do routes? get the toc in from schedule_extra"
			# XXX: optimize - merge/split same as services
			# XXX: route metadata
			route_key = stops[0].id, stops[-1].id
			if route_key in route_ids: route_id = route_ids[route_key]
			else:
				route_id = route_ids[route_key] = insert( 'gtfs.routes',
					route_short_name=f'{stops[0].crs_code}-{stops[-1].crs_code}',
					route_long_name='From {} to {}'.format(*(
						(stops[n].description.title() or stops[n].crs_code) for n in [0, -1] )),
					route_type=int(GTFSRouteType.rail) )

			## Trip and its stops
			#
			#  - Trips are considered equal (via trip_hash) for gtfs purposes
			#    when it's same cif.train_uid and same stops at the same days/times.
			#  - Again, it's not enough to just check train_uid, as schedules
			#    with same cif.train_uid can have different gtfs.stop sequences.
			#  - If additional metadata will be added to gtfs.trips,
			#    might be necessary to hash it as well, or just get rid of dedup here.

			trip_hash, trip_stops = [s.train_uid], list()
			svc_span = GTFSTimespan(
				s.runs_from, s.runs_to,
				except_days=s.bank_holiday_running and self.bank_holidays,
				weekdays=tuple(getattr(s, k) for k in GTFSTimespan.weekday_order) )

			## Process stop times
			# XXX: time conversions - DTD->GTFS and late->next-day
			for n, st in enumerate(stops, 1):

				public_stop = st.public_arrival_time or st.public_departure_time
				if public_stop:
					pickup_type = GTFSPickupType.regular
					ts_arr, ts_dep = st.public_arrival_time, st.public_departure_time
				else:
					pickup_type = GTFSPickupType.none
					ts_arr, ts_dep = st.scheduled_arrival_time, st.scheduled_departure_time
				# XXX: check if origin has some arrival time indication in CIF data
				ts_arr, ts_dep = ts_arr or ts_dep, ts_dep or ts_arr # origin/termination stops

				trip_stops.append((st, pickup_type, ts_arr, ts_dep))
				trip_hash.append(( st.stop_id,
					ts_arr and ts_arr.total_seconds(), ts_dep and ts_dep.total_seconds() ))

			## Check if such gtfs.trip already exists and only needs gtfs.calendar entry
			trip_hash = hash(tuple(trip_hash))
			if trip_hash in trip_merge_idx:
				stats['trip-dedup'] += 1
				trip_id = trip_merge_idx[trip_hash]
			else:
				trip_id = trip_merge_idx[trip_hash] = len(trip_merge_idx)
				# XXX: check if more trip/stop metadata can be filled-in here
				insert( 'gtfs.trips',
					trip_id=trip_id, route_id=route_id, service_id=s.id,
					trip_headsign=s.train_uid, trip_short_name=s.retail_train_id )
				for st, pickup_type, ts_arr, ts_dep in trip_stops:
					insert( 'gtfs.stop_times',
						trip_id=trip_id,
						arrival_time=ts_arr, departure_time=ts_dep,
						stop_id=st.stop_id, stop_sequence=n,
						pickup_type=int(pickup_type) )
			trip_svc_timespans[trip_id].append(svc_span)

		stats['trip-count'] = len(trip_svc_timespans)


		## Step-2: merge/optimize timespans in trip_svc_timespans where possible

		svc_id_seq = iter(range(1, 2**30))
		svc_merge_idx = dict() # {svc_span: svc_id} - to deduplicate svc_id for diff trips

		self.log.debug('Merging service timespans for {} gtfs.trips...', len(trip_svc_timespans))
		for trip_id, spans in trip_svc_timespans.items():

			# Merge timespans where possible
			if len(spans) > 1:
				merged, spans_merged = None, list()
				for s1, s2 in it_ngrams(sorted(spans), 2):
					if merged: # s1 was already used in previous merge
						merged = None
						continue
					merged = s1.merge(s2)
					if merged: stats['svc-merge-op'] += 1
					spans_merged.append(merged or s1)
				if merged is None: spans_merged.append(s2)
				if len(spans) != len(spans_merged): stats['svc-merge'] += 1
				spans = trip_svc_timespans[trip_id] = spans_merged

			# Store spans into gtfs.calendar/gtfs.calendar_dates, assigning service_id to each
			for n, span in enumerate(spans):
				if span in svc_merge_idx: # reuse service_id for same exact span from diff trip
					stats['svc-dedup'] += 1
					spans[n] = svc_id
					continue
				svc_id = spans[n] = svc_merge_idx[span] = next(svc_id_seq)
				insert( 'gtfs.calendar', service_id=svc_id,
					start_date=span.start, end_date=span.end, **span.weekday_dict )
				for day in span.except_days:
					insert( 'gtfs.calendar_dates', service_id=svc_id,
						date=day, exception_type=int(GTFSExceptionType.removed) )

		stats['svc-count'] = len(svc_merge_idx)


		## Step-3: Store assigned service_id to gtfs.trips,
		##  duplicating trip where there's >1 service_id associated with it.

		trip_id_seq = max(trip_svc_timespans or [0]) + 1
		trip_id_seq = iter(range(trip_id_seq, trip_id_seq + 2**30))

		self.log.debug('Updating service_id in gtfs.trips table...')
		for trip_id, svc_id_list in trip_svc_timespans.items():
			q('UPDATE gtfs.trips SET service_id = %s WHERE trip_id = %s', svc_id_list[0], trip_id)
			if len(svc_id_list) > 1:
				trip, = q('SELECT * FROM gtfs.trips WHERE trip_id = %s', trip_id)
				trip_stops = q('SELECT * FROM gtfs.stop_times WHERE trip_id = %s', trip_id)
				trip, trip_stops = trip._asdict(), list(s._asdict() for s in trip_stops)
				stats['trip-row-dup'] += 1
				for svc_id in svc_id_list[1:]:
					stats['trip-row-dup-op'] += 1
					trip_id = next(trip_id_seq)
					trip.update(id=None, trip_id=trip_id)
					insert('gtfs.trips', **trip)
					for st in trip_stops:
						st.update(id=None, trip_id=trip_id)
						insert('gtfs.stop_times', **st)

		stats_override_counts = list()
		stats.update({'train-count': len(stats_by_train), 'train-with-override': 0})
		for train_uid, train_stats in stats_by_train.items():
			sched_override_count = sum(
				v for k,v in train_stats.items() if k != 'stp-P' and k.startswith('stp-') )
			stats_override_counts.append(sched_override_count)
			if sched_override_count > 0: stats['train-with-override'] += 1
		stats.update({ 'train-override-median':
			sum(stats_override_counts) / len(stats_override_counts) })

		log_lines(log.debug, ['Stats:', *(
			'  {{}}: {}'.format('{:,}' if isinstance(v, int) else '{:.1f}').format(k, v)
			for k,v in sorted(stats.items()) )])


def main(args=None):
	import argparse
	parser = argparse.ArgumentParser(
		description='Tool to convert imported DTD/CIF data'
			' stored in one MySQL db to GTFS feed in another db.')

	group = parser.add_argument_group('MySQL db parameters')
	group.add_argument('-s', '--src-cif-db',
		default='cif', metavar='db-name',
		help='Database name to read CIF data from (default: %(default)s).')
	group.add_argument('-d', '--dst-gtfs-db',
		default='gtfs', metavar='db-name',
		help='Database name to store GTFS feed to (default: %(default)s).')
	group.add_argument('-f', '--mycnf-file',
		metavar='path', default=str(pathlib.Path('~/.my.cnf').expanduser()),
		help='Alternative ~/.my.cnf file to use to read all connection parameters from.'
			' Parameters there can include: host, port, user, passwd, connect,_timeout.'
			' Overidden parameters:'
				' db (specified via --src-cif-db/--dst-gtfs-db options),'
				' charset=utf8mb4 (for max compatibility).')
	group.add_argument('-g', '--mycnf-group', metavar='group',
		help='Name of "[group]" (ini section) in ~/.my.cnf ini file to use parameters from.')

	group = parser.add_argument_group('Extra data sources')
	group.add_argument('-e', '--uk-bank-holiday-list',
		metavar='file', default='doc/UK-bank-holidays.csv',
		help='List of dates, one per line, for UK bank holidays. Default: %(default)s')
	group.add_argument('--uk-bank-holiday-fmt',
		metavar='strptime-format', default='%d-%b-%Y',
		help='strptime() format for each line in -e/--uk-bank-holiday-list file. Default: %(default)s')

	group = parser.add_argument_group('Misc other options')
	group.add_argument('-n', '--test-schedule-limit', type=int, metavar='n',
		help='Do test-run with specified number of schedules only.'
			' This always produces incorrect results, only useful for testing the code quickly.')
	group.add_argument('-v', '--verbose', action='store_true',
		help='Print info about non-critical errors and quirks found during conversion.')
	group.add_argument('--debug', action='store_true', help='Verbose operation mode.')

	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	global log
	if opts.debug: log = logging.DEBUG
	elif opts.verbose: log = logging.INFO
	else: log = logging.WARNING
	logging.basicConfig( level=log, datefmt='%Y-%m-%d %H:%M:%S',
		format='%(asctime)s :: %(name)s %(levelname)s :: %(message)s' )
	log = get_logger('main')

	bank_holidays = set()
	if opts.uk_bank_holiday_list:
		with pathlib.Path(opts.uk_bank_holiday_list).open() as src:
			for line in src.read().splitlines():
				bank_holidays.add(datetime.datetime.strptime(line, opts.uk_bank_holiday_fmt).date())

	# Useful stuff for ~/.my.cnf: host port user passwd connect_timeout
	mysql_conn_opts = dict(filter(op.itemgetter(1), dict(
		read_default_file=opts.mycnf_file, read_default_group=opts.mycnf_group ).items()))
	with DTDtoGTFS(
			opts.src_cif_db, opts.dst_gtfs_db,
			mysql_conn_opts, bank_holidays ) as conv:
		conv.run(opts.test_schedule_limit)

if __name__ == '__main__': sys.exit(main())
