#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
import os, sys, contextlib, logging, pathlib, re, warnings, locale
import collections, enum, math, time, datetime

import pymysql, pymysql.cursors # https://pymysql.readthedocs.io/


## Compatibility stuff for pypy3-5.8.0/python-3.5.x
if sys.version_info >= (3, 6, 0): import secrets
else:
	import base64
	def token_urlsafe(chars=4):
		return base64.urlsafe_b64encode(
			os.urandom(chars * 6 // 8 + 1) ).rstrip(b'=').decode()[:chars]
	secrets = type('module', (object,), dict(token_urlsafe=token_urlsafe))


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

iter_chain = it.chain.from_iterable
list_chain = lambda v: list(iter_chain(v))


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
			for row in rows: yield row


class ConversionError(Exception): pass
class CIFError(ConversionError): pass


one_day = datetime.timedelta(days=1)

def dts_format(dts, sec=False):
	if isinstance(dts, str): return dts
	if isinstance(dts, datetime.timedelta): dts = dts.total_seconds()
	dts_days, dts = divmod(int(dts), 24 * 3600)
	dts = str(datetime.time(dts // 3600, (dts % 3600) // 60, dts % 60, dts % 1))
	if not sec: dts = dts.rsplit(':', 1)[0]
	if dts_days: dts = '{}+{}'.format(dts_days, dts)
	return dts

GTFSRouteType = enum.IntEnum( 'RouteType',
	'light_rail subway rail bus ferry cable_car gondola funicular spacecraft', start=0 )
GTFSPickupType = enum.IntEnum('PickupType', 'regular none phone driver', start=0)
GTFSExceptionType = enum.IntEnum('ExceptionType', 'added removed')

TimespanMerge = collections.namedtuple('TSMerge', 't span')
TimespanMergeType = enum.IntEnum( 'TSMergeType',
	'none same inc inc_diff_weekdays overlap bridge', start=0 )
TimespanDiff = collections.namedtuple('TSMerge', 't spans')
TimespanDiffType = enum.IntEnum('TSDiffType', 'none full split move exc', start=0)

class TimespanEmpty(Exception): pass

@ft.total_ordering
class Timespan:

	weekday_order = 'monday tuesday wednesday thursday friday saturday sunday'.split()

	def __init__(self, start, end, weekdays=[1]*7, except_days=None):
		self.start, self.end, self.except_days = start, end, set(except_days or list())
		if isinstance(weekdays, dict): weekdays = (weekdays[k] for k in self.weekday_order)
		self.weekdays = tuple(map(int, weekdays))
		try: self.start, self.end = next(self.date_iter()), next(self.date_iter(reverse=True))
		except StopIteration: raise TimespanEmpty(str(self)) from None
		self.except_days = frozenset(filter(
			( lambda day: self.start <= day <= self.end
				and self.weekdays[day.weekday()] ), self.except_days ))
		# self.end is negated in hash_tuple so that largest interval with same start is sorted first
		self._hash_tuple = (
			self._date_int(self.start), -self._date_int(self.end), self.weekdays, self.except_days )

	def _date_int(self, day):
		return day.year*10000 + day.month*100 + day.day

	def __lt__(self, span): return self._hash_tuple < span._hash_tuple
	def __eq__(self, span): return self._hash_tuple == span._hash_tuple
	def __hash__(self): return hash(self._hash_tuple)

	def __repr__(self):
		weekdays = ''.join((str(n) if d else '.') for n,d in enumerate(self.weekdays, 1))
		except_days = ', '.join(sorted(map(str, self.except_days)))
		return f'<TS {weekdays} [{self.start} {self.end}] {{{except_days}}}>'

	@property
	def weekday_dict(self): return dict(zip(self.weekday_order, self.weekdays))

	def date_iter(self, reverse=False):
		'Iterator for all valid (non-weekend/excluded) dates in this timespan.'
		return self.date_range( self.start, self.end,
			self.weekdays, self.except_days, reverse=reverse )

	@staticmethod
	def date_iter_set(span_set):
		return sorted(set(iter_chain(s.date_iter() for s in span_set)))

	@staticmethod
	def date_range(a, b, weekdays=None, except_days=None, reverse=False):
		if a > b: return
		if reverse: a, b = b, a
		svc_day_check = ( lambda day, wd=weekdays or [1]*7,
			ed=except_days or set(): wd[day.weekday()] and day not in ed )
		for day in filter(svc_day_check, iter_range(a, b, one_day)): yield day

	@staticmethod
	def date_overlap(s1, s2):
		'Sort timespans by start/end and return overlapping range, if any.'
		if s1 > s2: s1, s2 = s2, s1 # s1 starts first and ends last if start is same
		a, b = ((s2.start, min(s1.end, s2.end)) if s1.end >= s2.start else (None, None))
		return s1, s2, a, b

	@classmethod
	def exc_days_overlap(cls, s1, s2, a=None, b=None):
		if not (a and b): s1, s2, a, b = self.date_overlap(s1, s2)
		exc_intersect = s1.except_days & s2.except_days
		return filter(
			lambda day: (day < a or day > b) or day in exc_intersect,
			s1.except_days | s2.except_days )

	def merge(self, span, exc_days_to_split=10):
		'''Return TimespanMerge value,
				with either new merged timespan or None if it's not possible.
			exc_days_to_split = number of days in sequence
				to tolerate as exceptions when merging two spans with small gap in-between.'''
		(s1, s2, a, b), weekdays = self.date_overlap(self, span), self.weekdays
		if s1.weekdays != s2.weekdays:
			# Check if s1 includes s2 range and all its weekdays
			if ( b == s2.end
					and tuple(d1|d2 for d1,d2 in zip(s1.weekdays, s2.weekdays)) == s1.weekdays
					and s1.except_days.issuperset(s2.except_days) ):
				return TimespanMerge( TimespanMergeType.inc_diff_weekdays,
					Timespan(s1.start, s1.end, s1.weekdays, self.exc_days_overlap(s1, s2, a, b)) )
		else:
			if s1 == s2: return TimespanMerge(TimespanMergeType.same, s1)
			if a and b: # overlap
				return TimespanMerge(TimespanMergeType.overlap, Timespan(
					s1.start, max(s1.end, s2.end), weekdays, self.exc_days_overlap(s1, s2, a, b) ))
			if len(list(self.date_range(s1.end, s2.start, weekdays))) <= exc_days_to_split: # bridge
				return TimespanMerge(TimespanMergeType.bridge, Timespan(
					s1.start, max(s1.end, s2.end), weekdays,
					set( s1.except_days | s2.except_days |
						set(self.date_range(s1.end + one_day, s2.start - one_day, weekdays)) ) ))
		return TimespanMerge(TimespanMergeType.none, None)

	@classmethod
	def merge_set(cls, spans):
		if len(spans) <= 1: return TimespanMerge(list(), spans)
		span_set, merge_ops = set(spans), list()
		while True:
			span_merge_count = len(span_set)
			for s1, s2 in list(it.combinations(span_set, 2)):
				if not span_set.issuperset([s1, s2]): continue
				merge = s1.merge(s2)
				if merge.t:
					span_set.difference_update([s1, s2])
					span_set.add(merge.span)
					merge_ops.append(merge.t)
			if len(span_set) == span_merge_count: break
		if len(span_set) == len(spans): return TimespanMerge(list(), spans)
		return TimespanMerge(merge_ops, list(span_set))

	def _difference_range(self, span, exc_days_to_split=10):
		'''Try to subtract overlap range from timespan,
				either by moving start/end or splitting it in two.
			Returns either TimespanDiff or None if it cannot be done this way.'''
		if tuple(d1|d2 for d1,d2 in zip(self.weekdays, span.weekdays)) != span.weekdays: return
		s1, s2, a, b = self.date_overlap(self, span)
		if not (a and b): return TimespanDiff(TimespanDiffType.none, [self])
		svc_days = set(self.date_range(a, b, self.weekdays, self.except_days))
		if span.except_days.issuperset(svc_days): # all service days are exceptions in overlay
			return TimespanDiff(TimespanDiffType.none, [self])
		if svc_days.intersection(span.except_days): return # XXX: need "added" exceptions here
		start, end = self.start, self.end
		if a == start and b == end: return TimespanDiff(TimespanDiffType.full, [])
		if a != start and b != end: # split in two, unless only few exc_days required to bridge
			if len(svc_days.difference(span.except_days)) >= exc_days_to_split:
				return TimespanDiff(TimespanDiffType.split, [
					Timespan(start, span.start - one_day, self.weekdays, self.except_days),
					Timespan(span.end + one_day, end, self.weekdays, self.except_days) ])
			return
		# Remaining case is (a == start or b == end) - move start/end accordingly
		if a == start: start = b + one_day
		else: end = a - one_day
		return TimespanDiff( TimespanDiffType.move,
			[Timespan(start, end, self.weekdays, self.except_days)] )

	def difference(self, span):
		'''Return new timespan(s) with specified one excluded from it on None if there is no overlap.
			exc_days_to_split = number of days in sequence
				to tolerate as exceptions before splitting this span into two.'''
		# Try to adjust start/end dates of timespan or split it in two
		diff = self._difference_range(span)
		if diff: return diff
		# Otherwise add all days of the overlay timespan as exceptions
		try:
			span_diff = Timespan( self.start, self.end,
				self.weekdays, self.except_days | frozenset(span.date_iter()) )
		except TimespanEmpty: return TimespanDiff(TimespanDiffType.full, [])
		if span_diff == self: return TimespanDiff(TimespanDiffType.none, [self])
		return TimespanDiff(TimespanDiffType.exc, [span_diff])

	@classmethod
	def difference_set(cls, spans1, spans2):
		if isinstance(spans2, cls): spans2 = [spans2]
		diff_types = list()
		for span in spans2:
			diffs = list(s.difference(span) for s in spans1)
			diffs_ext = list(map(op.attrgetter('t'), diffs))
			if any(diffs_ext):
				spans1 = list_chain(map(op.attrgetter('spans'), diffs))
				diff_types.extend(filter(None, diffs_ext))
		return TimespanDiff(diff_types, spans1)

	def intersection(self, span):
		'Returns None or single span corresponding to intersection.'
		s1, s2, a, b = self.date_overlap(self, span)
		if not (a and b): return
		try:
			return Timespan( a, b,
				tuple(d1&d2 for d1,d2 in zip(s1.weekdays, s2.weekdays)),
				s1.except_days | s2.except_days )
		except TimespanEmpty: return # due to weekdays/exceptions

	@classmethod
	def intersection_set(cls, spans1, spans2):
		return list(filter( None,
			(s1.intersection(s2) for s1,s2 in it.product(spans1, spans2)) ))

	@classmethod
	def shift_set(cls, spans, offset):
		if not spans or offset == 0: return spans
		offset_days = offset * one_day
		return list(Timespan(
			span.start + offset_days, span.end + offset_days,
			tuple(span.weekdays[(n-offset)%7] for n in range(7)),
			set(day + offset_days for day in span.except_days) ) for span in spans)


ScheduleStop = collections.namedtuple('SchedStop', 'id ts_arr ts_dep')
ScheduleSet = collections.namedtuple('SchedSet', 'train_uid sched_list')

class Schedule:

	def __init__(self, train_uid, stops, spans, **meta):
		self.train_uid, self.stops, self.spans, self.meta = train_uid, tuple(stops), spans, meta
		self._hash_tuple = self.train_uid, self.stops

	def __eq__(self, s): return self._hash_tuple == s._hash_tuple
	def __hash__(self): return hash(self._hash_tuple)

	def copy(self, **updates):
		state = self.meta.copy()
		state.update((k, getattr(self, k)) for k in 'train_uid stops spans'.split())
		state.update(updates)
		return Schedule(**state)

	def subtract_timespan(self, span):
		diff_types, self.spans = Timespan.difference_set(self.spans, span)
		return diff_types

	def partition(self, spans):
		'Return timespans for intersection and difference from specified set of spans.'
		diffs = Timespan.difference_set(self.spans, spans)
		overlaps = Timespan.intersection_set(self.spans, spans)
		return overlaps, diffs.spans


AssocType = enum.IntEnum('AssocType', 'split join')
AssocQuirk = enum.IntEnum('AssocApply', 'no_stop no_base')
AssocApply = collections.namedtuple('AssocApply', 'quirks scheds')

class Association:

	def __init__(self, base, assoc, t, stop, assoc_next_day, spans):
		self.base, self.assoc, self.t, self.stop, self.spans = base, assoc, t, stop, spans
		self.spans_assoc_offset = int(bool(assoc_next_day))
		self.spans_assoc = Timespan.shift_set(self.spans, self.spans_assoc_offset)
		self._hash_tuple = self.base, self.assoc, self.stop

	def __eq__(self, assoc): return self._hash_tuple == assoc._hash_tuple
	def __hash__(self): return hash(self._hash_tuple)

	def subtract_timespan(self, span):
		diff_types, self.spans = Timespan.difference_set(self.spans, span)
		return diff_types

	def apply(self, scheds_base, scheds_assoc):
		'''Return new list of Schedules with this Association applied to scheds_assoc,
			using scheds_base (can be empty) as a base train Schedules for assoc timespan(s).'''
		quirks, sched_res = list(), list()
		if not scheds_base:
			quirks.append(AssocQuirk.no_base)
			scheds_base = [None]
		for sched, sched_base in it.product(scheds_assoc, scheds_base):
			if sched_base:
				spans, diffs = sched.partition(sched_base.spans)
				if not spans: continue
				# XXX: can base schedules change in assoc timespan?
				assert not diffs, [self.base, self.assoc, Timespan.merge_set(spans).span, diffs]
			else: spans = sched.spans
			head, tail = sched_base.stops if sched_base else list(), sched.stops
			if self.t == AssocType.split: head, tail = tail, head
			stop_check = lambda s,chk_id=self.stop: s.id != chk_id
			stops = list(it.chain(
				it.takewhile(stop_check, head), it.dropwhile(stop_check, tail) ))
			if len(stops) < 2:
				quirks.append(AssocQuirk.no_stop)
				continue
			sched_res.append(sched.copy(
				stops=stops, spans=spans,
				train_uid=f'{self.assoc}_{self.base}' ))
		return AssocApply(quirks, sched_res)


class DTDtoGTFS:

	# Forces errors on truncated values and issues in multi-row inserts
	sql_mode = 'strict_all_tables'

	def __init__( self,
			db_cif, db_gtfs, conn_opts_base, bank_holidays,
			db_gtfs_schema=None, db_gtfs_mem=None,
			db_noop=False, db_nocommit=False ):
		self.db_cif, self.db_gtfs, self.db_names = db_cif, db_gtfs, dict(cif=db_cif, gtfs=db_gtfs)
		self.db_noop, self.db_nocommit = db_noop, db_nocommit
		self.db_gtfs_schema, self.db_gtfs_mem = db_gtfs_schema, db_gtfs_mem
		self.conn_opts_base, self.bank_holidays = conn_opts_base, bank_holidays
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

		c = self.c = self.connect()
		self.db = self.db_conns[None]
		if self.db_gtfs_schema:
			self.log.debug( 'Initializing gtfs database'
				' (name={}, memory-engine={}) tables...', self.db_gtfs, bool(self.db_gtfs_mem) )
			with open(self.db_gtfs_schema) as src: schema = src.read()
			if self.db_gtfs_mem:
				schema = re.sub(r'(?i)\bENGINE=\S+\b', 'ENGINE=MEMORY', schema)
			if not self.db_noop:
				# Not using "drop database if exists" here as it raises warnings
				c.execute( 'SELECT schema_name FROM'
					' information_schema.schemata WHERE schema_name=%s', self.db_gtfs )
				if list(c.fetchall()): c.execute(f'drop database {self.db_gtfs}')
				c.execute(f'create database {self.db_gtfs}')
				c.execute(f'use {self.db_gtfs}')
				c.execute(schema)
				self.commit(force=True)

		return self

	def __exit__(self, *exc):
		if self.ctx: self.ctx = self.ctx.close()
		self.db_conns = self.db_cursors = self.db = self.c = None


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
		if self.db_noop and re.search(r'(?i)^\s*(insert|delete|update)', q): return
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
		return res if not fetch else (res and list(res))

	def insert(self, table, **row):
		if self.db_noop: return
		db, table = table.split('.', 1)
		row = collections.OrderedDict(row.items())
		cols, vals = ','.join(row.keys()), ','.join(['%s']*len(row))
		return self.qb(
			f'INSERT INTO {self.db_names[db]}.{table} ({cols}) VALUES ({vals})',
			*row.values(), fetch=False )

	def commit(self, force=False):
		if not force and (self.db_noop or self.db_nocommit): return
		for conn in self.db_conns.values(): conn.commit()

	def escape(self, val):
		return self.db.escape(val)


	def run(self, test_run_slice=None):
		self.stats = collections.Counter()
		self.stats_by_train = collections.defaultdict(collections.Counter)

		### These are both very straightforward copy from cif tables to gtfs
		self.populate_stops()
		self.populate_transfers()

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

		## Step-1: Populate gtfs.trips and gtfs.stop_times for cif.schedules,
		##  building mapping of service timespans for each trip_id.
		schedule_set_iter = self.get_schedules(test_run_slice)
		schedule_iter = self.apply_associations(schedule_set_iter)
		trip_svc_timespans = self.schedules_to_trips(schedule_iter)

		## Step-2: Merge/optimize timespans in trip_svc_timespans
		##  and create gtfs.calendar entries, converting these timespans to service_id values.
		trip_svc_ids = self.create_service_timespans(trip_svc_timespans)

		## Step-3: Store assigned service_id to gtfs.trips,
		##  duplicating trip where there's >1 service_id associated with it.
		self.assign_service_id_to_trips(trip_svc_ids)

		### Done!
		self.commit()
		if self.log.isEnabledFor(logging.DEBUG): self.log_stats()


	def populate_stops(self):
		# XXX: check if there are locations without crs code and what that means
		# XXX: convert physical_station eastings/northings to long/lat
		self.log.debug('Populating gtfs.stops table...')
		self.qb(f'''
			INSERT INTO {self.db_gtfs}.stops
				( stop_id, stop_code, stop_name, stop_desc,
					stop_timezone, wheelchair_boarding )
				SELECT
					crs_code, tiploc_code,
					station_name, station_name, "Europe/London", 0
				FROM {self.db_cif}.physical_station
				GROUP BY crs_code
				ORDER BY crs_code''')


	def populate_transfers(self):
		# XXX: better data source / convert to trip+frequences?
		# XXX: losing the mode here, TUBE, WALK, etc
		self.log.debug('Populating gtfs.transfers table...')
		self.qb(f'''
			INSERT INTO {self.db_gtfs}.transfers
				SELECT
					crs_code AS from_stop_id,
					crs_code AS to_stop_id,
					2 AS transfer_type,
					minimum_change_time * 60 AS duration
				FROM {self.db_cif}.physical_station
				UNION
				SELECT
					origin AS from_stop_id,
					destination AS to_stop_id,
					2 AS transfer_type,
					duration * 60 AS duration
				FROM {self.db_cif}.fixed_link
				UNION
				SELECT
					destination AS from_stop_id,
					origin AS to_stop_id,
					2 AS transfer_type,
					duration * 60 AS duration
				FROM {self.db_cif}.fixed_link''')


	def _get_schedules(self, test_run_slice, z_part=0.3):
		'''Iterator for raw cif.schedule rows from db.
			Schedules are fetched along with stops/stop_times.
			Two tables are queried in order: cif.schedule and cif.z_schedule.'''
		test_run_slice_parts = 0, 0 # only used with test_run_slice
		if isinstance(test_run_slice, int):
			train_uid_slice = f'''
				JOIN
					( SELECT DISTINCT(train_uid) AS train_uid
						FROM {self.db_cif}.{{z}}schedule rs
						JOIN
							(SELECT CEIL(RAND() *
								(SELECT MAX(id) FROM {self.db_cif}.{{z}}schedule)) AS id) rss
							ON rs.id >= rss.id
						GROUP BY train_uid HAVING COUNT(*) > 0
						LIMIT {{z_slice}} ) r
					ON r.train_uid = s.train_uid'''
			test_run_slice_parts = int(test_run_slice * z_part)
			test_run_slice_parts = test_run_slice - test_run_slice_parts, test_run_slice_parts
		elif test_run_slice:
			train_uid_slice = ','.join(map(self.escape, test_run_slice))
			train_uid_slice = f'''
				JOIN
					( SELECT DISTINCT(train_uid) AS train_uid
						FROM {self.db_cif}.{{z}}schedule rs
						WHERE rs.train_uid IN ({train_uid_slice})) r
					ON r.train_uid = s.train_uid'''
		else: train_uid_slice = ''

		# XXX: processing for train_category, platform, atoc_code, stop_id, etc
		q_sched = f'''
			SELECT
				{{z_id}} AS id, {{z_tuid}} AS retail_train_id,
				s.train_uid, runs_from, runs_to, bank_holiday_running,
				monday, tuesday, wednesday, thursday, friday, saturday, sunday,
				stp_indicator, location, crs_code, train_category,
				station_name, public_arrival_time, public_departure_time,
				scheduled_arrival_time, scheduled_pass_time, scheduled_departure_time,
				platform, atoc_code, st.id AS stop_id
			FROM {self.db_cif}.{{z}}schedule s
			{train_uid_slice}
			LEFT JOIN {self.db_cif}.schedule_extra e ON e.schedule = s.id
			LEFT JOIN {self.db_cif}.stop_time st ON st.schedule = s.id
			LEFT JOIN {self.db_cif}.physical_station ps ON st.location = ps.tiploc_code
			WHERE
				st.id IS NULL
				OR ( ps.crs_code IS NOT NULL
					AND (st.scheduled_arrival_time IS NOT NULL OR st.scheduled_departure_time IS NOT NULL) )
			ORDER BY s.train_uid, FIELD(s.stp_indicator,'P','O','N','C'), s.id, st.id'''
		q_sched_count = f'SELECT COUNT(*) FROM {self.db_cif}.{{z}}schedule s {train_uid_slice}'
		q_tweaks = list(
			dict(z=z, z_id=z_id, z_tuid=z_tuid, z_slice=z_slice)
			for (z, z_id, z_tuid), z_slice in zip([
				('', 's.id', 'retail_train_id'), ('z_', '500000 + s.id', 'null') ], test_run_slice_parts) )

		sched_counts = list(self.qb(q_sched_count.format(**qt))[0][0] for qt in q_tweaks)
		self.stats['sched-count-z'] = sched_counts[1]
		self.log.debug('Schedule counts: regular={}, z={}', *sched_counts)
		yield sum(sched_counts)

		for sched_count, qt in zip(sched_counts, q_tweaks):
			self.log.debug(
				'Fetching cif.{}schedule entries (count={}, test-train-limit={}/{})...',
				qt['z'], sched_count, qt['z_slice'] or 'NA', test_run_slice )
			yield from self.q(q_sched.format(**qt))

	def get_schedules(self, test_run_slice):
		'''Iterate over cif.schedule entries and their stop_times in db,
			applying all overlays/cancellations (on a per-train basis)
			and yielding non-overlapping ScheduleSet entries for each train_uid.'''
		schedule_rows = self._get_schedules(test_run_slice)
		sched_count = next(schedule_rows)

		# Raw sql rows are grouped by:
		#  - train_uid (schedules) to apply overrides (stp=O/N/C) in the specified order.
		#  - schedule_id (stops) to get clean "process one schedule at a time" loop.
		self.log.debug('Processing {} cif.schedule entries...', sched_count)
		progress = progress_iter(self.log, 'schedules', sched_count)

		for train_uid, train_schedule_stops in it.groupby(schedule_rows, op.attrgetter('train_uid')):
			train_schedules = list()

			for schedule_id, stops in it.groupby(train_schedule_stops, op.attrgetter('id')):
				s = next(stops)
				stops = [s, *stops]

				next(progress)
				self.stats['sched-count'] += 1
				self.stats[f'sched-entry-{s.stp_indicator}'] += 1
				self.stats_by_train[s.train_uid][f'stp-{s.stp_indicator}'] += 1

				if s.stp_indicator not in 'PONC':
					self.log.warning( 'Skipping schedule entry with'
						' unrecognized stp_indicator value {!r}: {}', s.stp_indicator, s )
					continue

				try:
					svc_span = Timespan( s.runs_from, s.runs_to,
						weekdays=tuple(getattr(s, k) for k in Timespan.weekday_order),
						except_days=self.bank_holidays
							if s.stp_indicator == 'P' and not s.bank_holiday_running else None )
				except TimespanEmpty:
					self.stats['svc-empty-sched'] += 1
					continue

				if s.stp_indicator in 'ONC':
					for schedule in train_schedules:
						diff_types = schedule.subtract_timespan(svc_span)
						for dt in filter(None, diff_types):
							self.stats[f'svc-diff-op'] += 1
							self.stats[f'svc-diff-{dt.name}'] += 1
					if s.stp_indicator == 'C': continue # no associated stops, unlike stp=O/N

				### Processing for schedule stops

				if s.location is None:
					# self.log.info('Skipping schedule with no usable stops: {}', s)
					self.stats['sched-without-stops'] += 1
					continue

				sched_stops = list()

				# XXX: time conversions - DTD->GTFS
				ts_prev, ts_origin = None, min(filter(None, it.chain.from_iterable(
					[ st.scheduled_arrival_time, st.scheduled_departure_time,
						st.scheduled_pass_time, st.public_arrival_time, st.public_departure_time ]
					for st in stops )))
				for st in stops:
					ts_arr, ts_dep = st.public_arrival_time, st.public_departure_time
					# XXX: check if origin has some arrival time indication in CIF data
					ts_arr, ts_dep = ts_arr or ts_dep, ts_dep or ts_arr # origin/termination stops

					# Midnight rollover and sanity check
					if ts_arr and ts_dep:
						if (ts_prev or ts_origin) > ts_arr: ts_arr += one_day
						if ts_dep < ts_arr: ts_dep += one_day
						ts_prev = ts_dep

					sched_stops.append(ScheduleStop(st.crs_code, ts_arr, ts_dep))

				sched = Schedule( s.train_uid, sched_stops, [svc_span],
					trip_short_name=s.retail_train_id, trip_headsign=s.train_uid )
				train_schedules.append(sched)

			yield ScheduleSet(train_uid, train_schedules)


	def get_assoc_map(self):
		'''Build a map for association graphs, where for every train_uid
				that has any association info, list of corresponding association sets can be found.
			When all Schedules for train_uids in AssocSet.assoc_map are gathered, it can be processed.'''
		# XXX: look more closely into how same-day multi-base assoc records work
		self.log.debug('Constructing train association map...')

		### First associations are parsed and indexed by base_train_uid,
		###  then gathered into AssocSets - association graph for a set of trains.
		assoc_map = collections.defaultdict(lambda: (list(), list())) # {train_uid: [base, assoc]}
		assoc_types = dict(VV=AssocType.split, JJ=AssocType.join)
		assoc_list = self.qb(f'''
			SELECT *
			FROM {self.db_cif}.association a
			JOIN {self.db_cif}.tiploc tl ON a.assoc_location = tl.tiploc_code
			ORDER BY a.base_uid, a.assoc_uid, FIELD(a.stp_indicator,'P','O','N','C'), a.id''')
		for key, group in it.groupby(assoc_list, key=op.attrgetter('base_uid', 'assoc_uid')):
			trains_assoc = set() # records for base-assoc pair
			for a in group:
				at = assoc_types.get(a.assoc_cat) # stp=C rows can have empty type
				self.stats[ f'assoc-type-{at.name}'
					if at else 'assoc-type-{}'.format(a.assoc_cat or 'null') ] += 1
				if a.assoc_date_ind not in [None, 'N', 'S']:
					self.stats[f'assoc-date-ind-{a.assoc_date_ind}'] += 1
				try:
					assoc_span = Timespan( a.start_date, a.end_date,
						weekdays=tuple(getattr(a, k) for k in Timespan.weekday_order) )
					assoc = Association( a.base_uid, a.assoc_uid,
						at, a.crs_code, a.assoc_date_ind == 'N', [assoc_span] )
				except TimespanEmpty:
					self.stats['assoc-empty-span'] += 1
					continue
				if a.stp_indicator in 'ONC':
					for assoc2 in trains_assoc:
						if assoc2 == assoc and a.stp_indicator != 'C':
							assoc2.spans.append(assoc_span)
						else: assoc2.subtract_timespan(assoc_span)
					if a.stp_indicator == 'C': continue
				if assoc.t and assoc not in trains_assoc: trains_assoc.add(assoc)
			for assoc in trains_assoc:
				# XXX: merge here is probably a bad idea, as assocs match w/ schedules
				# merge = Timespan.merge_set(assoc.spans)
				# if merge.t:
				# 	for mt in merge.t:
				# 		self.stats['assoc-merge-op'] += 1
				# 		self.stats[f'assoc-merge-op-{mt.name}'] += 1
				# 	self.stats['assoc-merge'] += 1
				# 	assoc.spans = merge.span
				for n, train_uid in enumerate([assoc.base, assoc.assoc]):
					assoc_map[train_uid][n].append(assoc)
		return assoc_map

	def apply_associations(self, schedule_sets):
		'''Iterate over Schedule entries,
			applying cif.associations (joins/splits) to them where necessary.'''
		assoc_map = self.get_assoc_map()
		assoc_base, assoc_scheds = collections.defaultdict(list), collections.defaultdict(list)

		### Buffer non-base Schedules that are used in associations
		# Relevant slices of base schedules are also stored
		#  in assoc_base to splice their stops into assoc ones later.
		for ss in schedule_sets:
			schedules, assoc_spans = ss.sched_list.copy(), list()
			assocs_base, assocs = assoc_map[ss.train_uid]
			for assoc, sched in it.product(assocs_base, schedules):
				overlaps, diffs = sched.partition(assoc.spans)
				if overlaps: assoc_base[assoc].append(sched.copy(spans=overlaps))
			for assoc, sched in it.product(assocs, schedules):
				overlaps, diffs = sched.partition(assoc.spans_assoc)
				if not overlaps: continue
				assoc_spans.extend(assoc.spans_assoc)
				assoc_scheds[assoc].append(sched.copy(
					spans=Timespan.shift_set(overlaps, -assoc.spans_assoc_offset) ))
			for sched in schedules: sched.subtract_timespan(assoc_spans)
			yield from filter(op.attrgetter('spans'), schedules)

		### Process/flush buffered schedules after applying associations to them
		for assoc, scheds in assoc_scheds.items():
			assoc_res = assoc.apply(assoc_base[assoc], scheds)
			for quirk in assoc_res.quirks: self.stats[f'assoc-quirk-{quirk.name}'] += 1
			yield from assoc_res.scheds

	def schedules_to_trips(self, schedules):
		'''Process Schedule entries, populating gtfs.trips and gtfs.stop_times.
			Returns "trip_svc_timespans" index of {trip_id: timespans},
				to populate gtfs.calendar and gtfs.calendar_dates tables after some merging.
			As gtfs.calendar is still empty, all created trips don't have valid service_id value set.'''
		trip_svc_timespans = collections.defaultdict(list) # {trip_id: timespans}

		# Only one gtfs.trip_id is created for
		#  same trip_hash (train_uid+stops+stop_times) via trip_merge_idx.
		route_merge_idx = dict() # {(src, dst}: id}
		trip_merge_idx = dict() # {trip_hash: gtfs.trip_id}

		for s in schedules:

				if len(s.stops) < 2: # XXX: find out what these represent
					self.stats['trip-no-stops'] += 1
					continue

				### Trip deduplication

				if s in trip_merge_idx:
					self.stats['trip-dedup'] += 1
					trip_id = trip_merge_idx[s]
					trip_svc_timespans[trip_id].extend(s.spans)

				else:
					trip_id = trip_merge_idx[s] = len(trip_merge_idx) + 1

					# XXX: insert route info
					route_key = s.stops[0].id, s.stops[-1].id
					if route_key in route_merge_idx: route_id = route_merge_idx[route_key]
					else: route_id = route_merge_idx[route_key] = len(route_merge_idx) + 1

					# XXX: check if more trip/stop metadata can be filled-in here
					self.insert( 'gtfs.trips',
						trip_id=trip_id, route_id=route_id, service_id=0, **s.meta )
					for n, st in enumerate(s.stops, 1):
						if not (st.ts_arr and st.ts_dep): continue # non-public stop
						self.insert( 'gtfs.stop_times',
							trip_id=trip_id, pickup_type=int(GTFSPickupType.regular),
							arrival_time=st.ts_arr, departure_time=st.ts_dep,
							stop_id=st.id, stop_sequence=n )
					trip_svc_timespans[trip_id].extend(s.spans)

		self.stats['trip-count'] = len(trip_svc_timespans)
		return trip_svc_timespans


	def create_service_timespans(self, trip_svc_timespans):
		'''Merge/optimize timespans for trips and create gtfs.calendar entries.
			Returns trip_svc_ids mapping for {trip_id: svc_id_list}.'''

		svc_id_seq = iter(range(1, 2**30))
		svc_merge_idx = dict() # {svc_span: svc_id} - to deduplicate svc_id for diff trips
		trip_svc_ids = dict() # {trip_id: svc_id_list}

		self.log.debug('Merging service timespans for {} gtfs.trips...', len(trip_svc_timespans))
		for trip_id, spans in trip_svc_timespans.items():
			spans = trip_svc_ids[trip_id] = spans.copy()

			### Merge timespans where possible
			merge = Timespan.merge_set(spans)
			if merge.t:
				for mt in merge.t:
					self.stats['svc-merge-op'] += 1
					self.stats[f'svc-merge-op-{mt.name}'] += 1
				self.stats['svc-merge'] += 1
				spans = trip_svc_ids[trip_id] = merge.span

			### Store into gtfs.calendar/gtfs.calendar_dates, assigning service_id to each
			for n, span in enumerate(spans):
				if span in svc_merge_idx: # reuse service_id for same exact span from diff trip
					self.stats['svc-dedup'] += 1
					spans[n] = svc_merge_idx[span]
					continue
				svc_id = spans[n] = svc_merge_idx[span] = next(svc_id_seq)
				self.insert( 'gtfs.calendar', service_id=svc_id,
					start_date=span.start, end_date=span.end, **span.weekday_dict )
				for day in sorted(span.except_days):
					self.stats['svc-except-days'] += 1
					self.insert( 'gtfs.calendar_dates', service_id=svc_id,
						date=day, exception_type=int(GTFSExceptionType.removed) )

			if len(spans) == 1: self.stats['trip-spans-1'] += 1
			elif not spans: self.stats['trip-spans-0'] += 1

		self.stats['svc-count'] = len(svc_merge_idx)
		return trip_svc_ids


	def assign_service_id_to_trips(self, trip_svc_ids):
		'''Update gtfs.trips with assigned service_id values,
			duplicating trip where there's >1 service_id associated with it.'''
		trip_id_seq = iter(range(max(trip_svc_ids, default=0) + 1, 2**30))
		self.log.debug('Updating service_id in gtfs.trips table...')
		for trip_id, svc_id_list in trip_svc_ids.items():
			if not svc_id_list: # all timespans for trip got cancelled somehow
				self.qb(f'DELETE FROM {self.db_gtfs}.trips WHERE trip_id = %s', trip_id)
				self.qb(f'DELETE FROM {self.db_gtfs}.stop_times WHERE trip_id = %s', trip_id)
				self.stats['trip-delete'] += 1
				continue
			self.qb( f'UPDATE {self.db_gtfs}.trips SET'
				' service_id = %s WHERE trip_id = %s', svc_id_list[0], trip_id)
			if len(svc_id_list) > 1:
				if not self.db_noop:
					trip, = self.qb(f'SELECT * FROM {self.db_gtfs}.trips WHERE trip_id = %s', trip_id)
					trip_stops = self.qb( 'SELECT * FROM'
						f' {self.db_gtfs}.stop_times WHERE trip_id = %s', trip_id )
					trip, trip_stops = trip._asdict(), list(s._asdict() for s in trip_stops)
				self.stats['trip-row-dup'] += 1
				for svc_id in svc_id_list[1:]:
					self.stats['trip-row-dup-op'] += 1
					trip_id = next(trip_id_seq)
					if self.db_noop: continue
					trip.update(trip_id=trip_id, service_id=svc_id)
					self.insert('gtfs.trips', **trip)
					for st in trip_stops:
						st['trip_id'] = trip_id
						self.insert('gtfs.stop_times', **st)


	def log_stats(self):
		stats_override_counts = list()
		self.stats['train-count'] = len(self.stats_by_train)
		for train_uid, train_stats in self.stats_by_train.items():
			sched_override_count = sum(
				v for k,v in train_stats.items() if k != 'stp-P' and k.startswith('stp-') )
			stats_override_counts.append(sched_override_count)
			if sched_override_count > 0: self.stats['train-with-override'] += 1
		if stats_override_counts:
			self.stats.update({ 'train-override-median':
				sum(stats_override_counts) / len(stats_override_counts) })
		log_lines(self.log.debug, ['Stats:', *(
			'  {{}}: {}'.format('{:,}' if isinstance(v, int) else '{:.1f}').format(k, v)
			for k,v in sorted(self.stats.items()) )])



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
	group.add_argument('-i', '--dst-gtfs-schema',
		metavar='path-to-schema.sql', nargs='?', const='doc/db-schema-gtfs.sql',
		help='Create/init destination database with schema from specified .sql file.'
			' If such database already exists, it will be dropped first!'
			' Default schema file path (if not specified as optional argument): %(default)s.')
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
	group.add_argument('-n', '--test-train-limit', type=int, metavar='n',
		help='Do test-run with specified number of randomly-selected trains only.'
			' This always produces incomplete results, only useful for testing the code quickly.')
	group.add_argument('--test-train-uid', metavar='uid-list',
		help='Do test-run with specified train_uid entries only. Multiple values are split by spaces.')
	group.add_argument('--test-memory-schema', action='store_true',
		help='Process schema dump passed to -s/--dst-gtfs-schema'
			' option and replace table engine with ENGINE=MEMORY.')
	group.add_argument('--test-no-output', action='store_true',
		help='Instead of populating destination schema with results, just drop these on the floor.')
	group.add_argument('--test-no-commit', action='store_true',
		help='Do not commit data transaction to destination tables.'
			' Has no effect on -i/--dst-gtfs-schema option')
	group.add_argument('-v', '--verbose', action='store_true',
		help='Print info about non-critical errors and quirks found during conversion.')
	group.add_argument('--debug', action='store_true', help='Verbose operation mode.')

	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	sys.stdout = open(sys.stdout.fileno(), 'w', 1) # for occasional debug print()'s to work
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

	if opts.test_train_uid:
		test_uids = opts.test_train_uid.split()
		if opts.test_train_limit: test_uids = test_uids[:opts.test_train_limit]
	else: test_uids = opts.test_train_limit

	# Useful stuff for ~/.my.cnf: host port user passwd connect_timeout
	mysql_conn_opts = dict(filter(op.itemgetter(1), dict(
		read_default_file=opts.mycnf_file, read_default_group=opts.mycnf_group ).items()))
	with DTDtoGTFS(
			opts.src_cif_db, opts.dst_gtfs_db, mysql_conn_opts, bank_holidays,
			db_gtfs_schema=opts.dst_gtfs_schema, db_gtfs_mem=opts.test_memory_schema,
			db_noop=opts.test_no_output, db_nocommit=opts.test_no_commit ) as conv:
		conv.run(test_uids)

if __name__ == '__main__': sys.exit(main())
