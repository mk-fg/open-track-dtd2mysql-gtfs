#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
import datetime as dt
import os, sys, pathlib, signal, locale, warnings
import contextlib, inspect, collections, enum, time
import asyncio, urllib.parse, json, re, base64
import random, secrets, bisect, hashlib
import textwrap, pprint, logging, logging.handlers

import aiohttp # http://aiohttp.readthedocs.io
import aiomysql # http://aiomysql.readthedocs.io


class TestConfig:

	## test_match: how to select API to match gtfs data (e.g. trip) against.
	##  all - query all APIs for match
	##  any - query one API at random
	##  first - query APIs until match is found
	test_match = enum.Enum('TestMatch', 'all any first').any
	test_match_parallel = 1 # number of APIs to query in parallel, only for test_match=all/first

	## test_pick_{trip,date}: weights for picking which trips/days to test first.
	## "seq" in all of these is a simple sequential pick.
	test_pick_trip = dict(seq=1, assoc=0.5, z=0.2)
	test_pick_date = dict(
		seq=0, seq_next_week=1e-5, # to avoid dates that are likely to change
		seq_after_next_week=1, bank_holiday=2, random=2 )

	## test_pick_special: use named iterator func for special trip/date selection.
	##   holidays - pick from bank_holidays list (can also be read from cli-specified file)
	test_pick_special = None
	test_pick_special_iters = {'bank-holidays-only': 'holidays'}

	## Misc picking options
	test_pick_date_set = None # only pick dates from the set
	test_pick_trip_random_order = True # randomize order of checked trips

	## test_train_uids: either integer to pick n random train_uids or list of specific uids to use.
	test_train_uids = None

	## test_trip_log: path to file to append tested trip_id's to and skip ones already there.
	test_trip_log = None

	# XXX: negative tests - specifically pick bank holidays and exception days
	test_trip_dates = 3 # how many dates to pick and test per trip (using test_pick_date weights)
	test_trip_embark_delay = dt.timedelta(seconds=20*60) # departure time offset for queries
	test_trip_journeys = 6 # api result count, should be high enough for direct trip to be there
	test_trip_time_slack = 0 # max diff in stop times to ignore

	trip_diff_cmd = 'diff -y' # for pretty-printing diffs between trip stops
	date_max_future_offset = 80 # don't pick future dates further than that
	bank_holidays = None

	## rate_*: parameters for introducing delays between tests to lessen api load.
	rate_interval = 1 # seconds
	rate_max_concurrency = 1 # only makes sense if tests take longer than rate_interval
	rate_min_seq_delay = 0 # seconds, only if rate_max_concurrency=1

	## mysql_*: database options, can also be specified via cli.
	mysql_db_name = 'gtfs'
	mysql_conn_opts = None
	mysql_sql_mode = 'strict_all_tables'

	## serw_*: southeasternrailway.co.uk api options.
	serw_api_url = 'https://api.southeasternrailway.co.uk'
	serw_crs_nlc_map = None
	serw_http_headers = {
		'Accept': 'application/json',
		'Content-Type': 'application/json',
		'Origin': 'https://ticket.southeasternrailway.co.uk',
		'x-access-token':
			'otrl|a6af56be1691ac2929898c9f68c4b49a0a2d930849770dba976be5d792a',
		'User-Agent': 'gtfs-webcheck/1.0 (+https://github.com/mk-fg/open-track-dtd2mysql-gtfs/)',
	}
	serw_error_skip = { # tuple of (failureType, errorCode), latter can be regexp
		# Skip any IPTIS-related warnings - should only be relevant for fares afaict
		('Warning', re.compile(r'^Iptis[A-Z]')) }
	serw_api_debug = False

	## debug_*: misc debug options - see corresponding command line parameters.
	debug_http_dir = None
	debug_cache_dir = None
	debug_trigger_mismatch = None

	def __init__(self, path=None):
		if path: self._update_from_file(path)
		self._debug_files = collections.Counter()

	def _update_from_file(self, path):
		import yaml # http://pyyaml.org/
		with path.open() as src: conf = yaml.safe_load(src)
		for k,v in conf.items():
			if k == 'test_match': v = self.test_match[v]
			if k == 'test_trip_embark_delay': v = dt.timedelta(seconds=v)
			setattr(self, k, v)


class AsyncExitStack:
	# Might be merged to 3.7, see https://bugs.python.org/issue29302
	# Implementation from https://gist.github.com/thehesiod/b8442ed50e27a23524435a22f10c04a0

	def __init__(self):
		self._exit_callbacks = collections.deque()

	def pop_all(self):
		new_stack = type(self)()
		new_stack._exit_callbacks = self._exit_callbacks
		self._exit_callbacks = collections.deque()
		return new_stack

	def push(self, exit_obj):
		_cb_type = type(exit_obj)
		try:
			exit_method = getattr(_cb_type, '__aexit__', None)
			if exit_method is None: exit_method = _cb_type.__exit__
		except AttributeError: self._exit_callbacks.append(exit_obj)
		else: self._push_cm_exit(exit_obj, exit_method)
		return exit_obj

	@staticmethod
	def _create_exit_wrapper(cm, cm_exit):
		if inspect.iscoroutinefunction(cm_exit):
			async def _exit_wrapper(exc_type, exc, tb):
				return await cm_exit(cm, exc_type, exc, tb)
		else:
			def _exit_wrapper(exc_type, exc, tb):
				return cm_exit(cm, exc_type, exc, tb)
		return _exit_wrapper

	def _push_cm_exit(self, cm, cm_exit):
		_exit_wrapper = self._create_exit_wrapper(cm, cm_exit)
		_exit_wrapper.__self__ = cm
		self.push(_exit_wrapper)

	@staticmethod
	def _create_cb_wrapper(callback, *args, **kwds):
		if inspect.iscoroutinefunction(callback):
			async def _exit_wrapper(exc_type, exc, tb): await callback(*args, **kwds)
		else:
			def _exit_wrapper(exc_type, exc, tb): callback(*args, **kwds)
		return _exit_wrapper

	def callback(self, callback, *args, **kwds):
		_exit_wrapper = self._create_cb_wrapper(callback, *args, **kwds)
		_exit_wrapper.__wrapped__ = callback
		self.push(_exit_wrapper)
		return callback

	def _shutdown_loop(self, *exc_details):
		received_exc = exc_details[0] is not None
		frame_exc = sys.exc_info()[1]
		def _fix_exception_context(new_exc, old_exc):
			while True:
				exc_context = new_exc.__context__
				if exc_context is old_exc: return
				if exc_context is None or exc_context is frame_exc: break
				new_exc = exc_context
			new_exc.__context__ = old_exc
		suppressed_exc = pending_raise = False
		while self._exit_callbacks:
			cb = self._exit_callbacks.pop()
			try:
				cb_result = yield cb(*exc_details)
				if cb_result:
					suppressed_exc, pending_raise = True, False
					exc_details = (None, None, None)
			except:
				new_exc_details = sys.exc_info()
				_fix_exception_context(new_exc_details[1], exc_details[1])
				pending_raise, exc_details = True, new_exc_details
		if pending_raise:
			try:
				fixed_ctx = exc_details[1].__context__
				raise exc_details[1]
			except BaseException:
				exc_details[1].__context__ = fixed_ctx
				raise
		return received_exc and suppressed_exc

	async def enter(self, cm):
		_cm_type = type(cm)
		_exit = getattr(_cm_type, '__aexit__', None)
		if _exit is not None: result = await _cm_type.__aenter__(cm)
		else:
			_exit = _cm_type.__exit__
			result = _cm_type.__enter__(cm)
		self._push_cm_exit(cm, _exit)
		return result

	async def close(self):
		await self.__aexit__(None, None, None)

	async def __aenter__(self): return self
	async def __aexit__(self, *exc_details):
		gen = self._shutdown_loop(*exc_details)
		try:
			result = next(gen)
			while True:
				try:
					if inspect.isawaitable(result): result = await result
					result = gen.send(result)
				except StopIteration: raise
				except BaseException as e: result = gen.throw(e)
		except StopIteration as e: return e.value


class AsyncStopIteration(Exception): pass

async def anext(aiter): # py3.7?
	async for v in aiter: return v
	raise AsyncStopIteration


class LogMessage(object):
	def __init__(self, fmt, a, k): self.fmt, self.a, self.k = fmt, a, k
	def __str__(self): return self.fmt.format(*self.a, **self.k) if self.a or self.k else self.fmt

class LogStyleAdapter(logging.LoggerAdapter):
	def __init__(self, logger, extra=None):
		super(LogStyleAdapter, self).__init__(logger, extra or {})
	def addHandler(self, handler, propagate=False):
		self.logger.propagate = propagate
		return self.logger.addHandler(handler)
	def log(self, level, msg, *args, **kws):
		if not self.isEnabledFor(level): return
		log_kws = {} if 'exc_info' not in kws else dict(exc_info=kws.pop('exc_info'))
		msg, kws = self.process(msg, kws)
		self.logger._log(level, LogMessage(msg, args, kws), (), log_kws)

class LogPrefixAdapter(LogStyleAdapter):
	def __init__(self, logger, prefix=None, prefix_raw=False, extra=None):
		if isinstance(logger, str): logger = get_logger(logger)
		if isinstance(logger, logging.LoggerAdapter): logger = logger.logger
		super(LogPrefixAdapter, self).__init__(logger, extra or {})
		if not prefix: prefix = get_uid()
		if not prefix_raw: prefix = '[{}] '.format(prefix)
		self.prefix = prefix
	def process(self, msg, kws):
		super(LogPrefixAdapter, self).process(msg, kws)
		return ('{}{}'.format(self.prefix, msg), kws)

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
	if n_max is None: return iter(it.repeat(None))
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

def random_weight(items, keys_subset=None):
	if isinstance(items, dict): items = items.items()
	keys = set(keys_subset or set())
	keys, weights = zip(*((k,v) for k,v in items if not keys or k in keys))
	return random.choices(keys, weights)[0]

popn = lambda v,n: list(v.pop() for m in range(n))
url_to_fn = lambda p: base64.urlsafe_b64encode(
	hashlib.blake2s(p.encode(), key=b'gtfs-webcheck.url-to-fn').digest() ).decode()[:8]
err_cls = lambda err: err.__class__.__name__

json_pretty = dict(sort_keys=True, indent=2, separators=(',', ': '))
pformat_data = lambda data: pprint.pformat(data, indent=2, width=100)
re_type = type(re.compile(''))

def die(): raise RuntimeError


class NTCursor(aiomysql.cursors.SSDictCursor):

	@staticmethod
	@ft.lru_cache(maxsize=128)
	def tuple_for_row(row):
		if isinstance(row, str): return lambda *a: tuple(a)
		row = list(k.replace('.', ' ').rsplit(None, 1)[-1] for k in row)
		return collections.namedtuple('Row', row, rename=True)

	def __init__(self, *args, tuple_type=None, **kws):
		self._tt = tuple_type
		super().__init__(*args, **kws)

	def __aiter__(self): return self

	async def close(self): # don't bother with unbuffered results here
		self._connection = None

	def _conv_row(self, row):
		if row is None: return
		return ( self._tt(*row) if self._tt else
			self.tuple_for_row(tuple(self._fields))(*row) )

	async def fetchall(self, bs=2**15):
		rows = list()
		while True:
			row_chunk = await self.fetchmany(bs)
			if not row_chunk: break
			rows.extend(row_chunk)
		return rows


class RateSemaphore:

	@classmethod
	def dummy(cls): return cls(0, 0, 2**30)

	def __init__(self, delay_s2s, delay_e2s=0, max_parallel=1, loop=None):
		'''Parameters:
			delay start-to-start - interval between starting new requests.
				Can be extended if "max_parallel" requests are already in progress.
			delay end-to-start - min delay from finishing last request and starting new one.
				Cannot be used with max_parallel > 1.
			max_parallel - how many requests are allowed to run in parallel.
				Only useful it delay_s2s is small enough that requests don't finish within it.'''
		assert max_parallel > 0 and delay_s2s >= 0 and delay_e2s >= 0
		assert max_parallel == 1 or not delay_e2s, [max_parallel, delay_e2s]
		self.s2s, self.e2s, self.n = delay_s2s, delay_e2s or 0, max_parallel
		self.loop = loop or asyncio.get_event_loop()
		self.ts, self.queue = self.loop.time(), asyncio.BoundedSemaphore(max_parallel)

	async def acquire(self):
		await self.queue.acquire()
		while True:
			delay = self.ts - self.loop.time()
			if delay <= 0:
				self.ts += self.s2s - delay
				return
			try: await asyncio.sleep(delay)
			except asyncio.CancelledError:
				self.queue.release()
				raise

	def release(self):
		self.queue.release()
		if self.e2s:
			ts_next = self.loop.time() + self.e2s
			if ts_next > self.ts: self.ts = ts_next

	def locked(self):
		return self.queue.locked() or self.loop.time() <= self.ts

	async def __aenter__(self): await self.acquire()
	async def __aexit__(self, *err): self.release()


class TimespanEmpty(Exception): pass

class Timespan:

	one_day = dt.timedelta(days=1)
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

	def __repr__(self):
		weekdays = ''.join((str(n) if d else '.') for n,d in enumerate(self.weekdays, 1))
		except_days = ', '.join(sorted(map(str, self.except_days)))
		return f'<TS {weekdays} [{self.start} {self.end}] {{{except_days}}}>'

	@property
	def weekday_dict(self): return dict(zip(self.weekday_order, self.weekdays))

	@classmethod
	def date_range(cls, a, b, weekdays=None, except_days=None, reverse=False):
		if a > b: return
		if reverse: a, b = b, a
		svc_day_check = ( lambda day, wd=weekdays or [1]*7,
			ed=except_days or set(): wd[day.weekday()] and day not in ed )
		for day in filter(svc_day_check, iter_range(a, b, cls.one_day)): yield day

	def date_iter(self, reverse=False, start=None):
		return self.date_range( start or self.start, self.end,
			self.weekdays, self.except_days, reverse=reverse )

GTFSEmbarkType = enum.IntEnum('EmbarkType', 'regular none phone driver', start=0)
GTFSExceptionType = enum.IntEnum('ExceptionType', 'added removed')

def dts_to_dt(dts, date=None):
	dts0 = dts
	if isinstance(dts, dt.timedelta): dts = dts.total_seconds()
	if isinstance(dts, dt.datetime):
		dts = dts.time()
		dts = dts.hour * 3600 + dts.minute * 60 + dts.second
	dts = int(dts)
	days, hours = divmod(dts // 3600, 24)
	ts = dt.time(hours, (dts % 3600) // 60, dts % 60, dts % 1)
	if date:
		if days: date += dt.timedelta(days)
		ts = dt.datetime.combine(date, ts)
	elif days: raise ValueError(f'>24h time: {dts0}')
	return ts


class GWCError(Exception): pass
class GWCAPIError(GWCError): pass
class GWCAPIErrorEmpty(GWCAPIError): pass
class GWCAPIErrorCode(GWCAPIError): pass

class GWCTestFoundDiffs(Exception): pass

class GWCTestSkip(Exception):
	def __init__(self, api, msg):
		self.api = api
		super().__init__(msg)
class GWCTestSkipTrip(GWCTestSkip): pass

class GWCTestFail(Exception):
	def __init__(self, api, trip, data, diff=None):
		self.api, self.trip, self.diff, self.data = api, trip, diff, data
class GWCTestFailNoJourney(GWCTestFail): pass
class GWCTestFailStopNotFound(GWCTestFail): pass
class GWCTestFailStopMismatch(GWCTestFail): pass

class GWCTestBatchFail(Exception):
	def __init__(self, *exc_list): self.exc_list = exc_list

GWCTestResult = collections.namedtuple('GWCTestResult', 'success exc')


class GWCTripStop:

	def __init__(self, crs, ts, pickup, dropoff, **meta):
		pickup, dropoff = map(GTFSEmbarkType, [pickup, dropoff])
		self.crs, self.ts, self.pickup, self.dropoff, self.meta = crs, ts, pickup, dropoff, meta

	def __repr__(self):
		embark = '-P'[self.pickup != GTFSEmbarkType.none] + '-D'[self.dropoff != GTFSEmbarkType.none]
		ts = str(self.ts) if self.ts else 'x'
		return f'<TS {self.crs} {self.meta.get("nlc", "x")} {embark} {ts}>'

class GWCTrip:

	TripSig = collections.namedtuple('TripSig', 'src dst train_uid_list ts_src')

	@classmethod
	def from_serw_cps(cls, sig, stops, links):
		embark_flags = dict(
			Normal=(True, True), Passing=(False, False),
			PickUpOnly=(True, False), SetDownOnly=(False, True),
			RequestStop=(False, GTFSEmbarkType.driver) )
		embark_flags_dict = {
			True: GTFSEmbarkType.regular, False: GTFSEmbarkType.none }
		src, dst, trip_stops, ts0 = sig.src, sig.dst, list(), None
		for stop in stops:
			stop_info = links[stop['station']]
			pickup, dropoff = embark_flags[stop['pattern']]
			ts = ( None if not (pickup or dropoff) else
				dt.datetime.strptime(stop['time']['scheduledTime'], '%Y-%m-%dT%H:%M:%S') )
			if ts:
				if not ts0: ts0 = dt.datetime(ts.year, ts.month, ts.day)
				ts -= ts0
			name, crs, nlc = op.itemgetter('name', 'crs', 'nlc')(stop_info)
			lat, lon = (stop_info.get(k) for k in ['latitude', 'longitude'])
			if src and src != crs: continue
			pickup, dropoff = (embark_flags_dict.get(v, v) for v in [pickup, dropoff])
			src = trip_stops.append(GWCTripStop(
				crs, ts, pickup, dropoff, name=name, nlc=nlc, lat=lat, lon=lon ))
			if dst == crs: break
		train_uid = ( sig.train_uid_list[0] if len(sig.train_uid_list) == 1
			else '{}_{}'.format(sig.train_uid_list[0], sig.train_uid_list[-1]) )
		return cls(train_uid, trip_stops, sig.ts_src, sig_train_uid_list=sig.train_uid_list)

	@classmethod
	def from_gtfs_stops(cls, train_uid, stops, ts_src=None):
		trip_stops = list(GWCTripStop( s.stop_id,
			s.departure_time, s.pickup_type, s.drop_off_type ) for s in stops)
		trip_stops = list(filter(lambda st: not (
			st.pickup == GTFSEmbarkType.none
			and st.dropoff == GTFSEmbarkType.none ), trip_stops))
		return cls(train_uid, trip_stops, ts_src)

	def __init__(self, train_uid, stops, ts_start=None, ts_end=None, **meta):
		self.train_uid, self.stops, self.meta = train_uid, stops, meta
		if stops and (ts_start or ts_end):
			stops_ts = list(filter(op.attrgetter('ts'), stops))
			if not ts_start:
				ts_start = dts_to_dt(stops_ts[0].ts, ts_end.date())
				while ts_start >= ts_end: ts_start -= dt.timedelta(days=1)
			if not ts_end:
				ts_end = dts_to_dt(stops_ts[-1].ts, ts_start.date())
				while ts_end <= ts_start: ts_end += dt.timedelta(days=1)
		self.ts_start, self.ts_end = ts_start, ts_end

	def __repr__(self):
		train_uld_list = self.meta.get('sig_train_uid_list') or ''
		if train_uld_list: # to show weird trip signatures with asterisks
			if set(train_uld_list) != set(self.train_uid.split('_')):
				train_uld_list = ' ({})'.format(' '.join(train_uld_list))
			else: train_uld_list = ''
		return (
			f'<Trip {self.train_uid}{train_uld_list}'
				f' [{self.ts_start or "-"} {self.ts_end or "-"}]'
				f' [{" - ".join(ts.crs for ts in self.stops)}]>' )

	def copy(self, **kws):
		state = dict((k, getattr(self, k)) for k in 'train_uid ts_start stops'.split())
		state.update(kws)
		return GWCTrip(**state)


class GWCJnSig:

	JnSigTrip = collections.namedtuple('JnSigTrip', 'src ts_src dst ts_dst')

	@classmethod
	def from_serw_url(cls, jn_sig_str):
		jn_sig = collections.deque(reversed(jn_sig_str.split('|')))
		ts_start, ts_end = cls._parse_times(jn_sig)
		trips, ts0 = list(), ts_start
		while jn_sig:
			t = jn_sig.pop().lower()
			if t == 'trip':
				src, dst = popn(jn_sig, 2)
				ts_src, ts_dst = cls._parse_times(jn_sig)
				assert ts_src >= ts0, [ts0, ts_src]
				rsid, rsid_prefix = popn(jn_sig, 2)
				trips.append(cls.JnSigTrip(src, ts_src, dst, ts_dst))
				ts0 = ts_dst
			elif t == 'transfer':
				src, dst, delta, tt = popn(jn_sig, 4)
				ts0 = ts0 + dt.timedelta(seconds=int(delta) * 60)
			else: raise NotImplementedError(t, jn_sig)
		return cls(trips, ts_start, ts_end)

	@classmethod
	def _parse_times(cls, jn_sig):
		ts1, ts2 = (
			dt.datetime.strptime(f'{d}-{t}', '%y%m%d-%H%M')
			for d,t in (popn(jn_sig, 2) for n in range(2)) )
		return ts1, ts2

	def __init__(self, trips, ts_start, ts_end):
		self.trips, self.ts_start, self.ts_end = trips, ts_start, ts_end

	def __repr__(self):
		stops = list()
		for jst in self.trips:
			src, ts_src, dst, ts_dst = jst
			ts_src, ts_dst = (ts.strftime('%H:%M') for ts in [ts_src, ts_dst])
			if not stops or src != stops[-1][0]: stops.append([src, ts_src])
			elif stops[-1][1] != ts_src: stops[-1][1] += f'/{ts_src}'
			stops.append([dst, ts_dst])
		stops = ' - '.join(f'{crs}[{ts}]' for crs, ts in stops)
		span = ' '.join(ts.strftime('%H:%M') for ts in [self.ts_start, self.ts_end])
		return f'<JnSig [{span}] [{stops}]>'

	def trip_index(self, src, dst):
		for n, t in enumerate(self.trips):
			if (src, dst) == (t.src, t.dst): return n, t
		raise IndexError(src, dst)


class GWCJn:

	@classmethod
	def from_serw_cps(cls, jn_sig, cps):
		if isinstance(jn_sig, str): jn_sig = GWCJnSig.from_serw_url(jn_sig)

		trip_order = list()
		for sig_key in cps['result']:
			# simple: 3087;3147;2017-08-14;P04621|14/08/2017
			# association: 3087;4892;2017-09-14;C20213|14/09/2017;OXF;C22477|14/09/2017
			# non-train: 3087;4892;2017-09-14;
			# Not sure what to do with date1/date2/... here.
			src, dst, date1, train_info = sig_key.split(';', 3)
			if not train_info: continue # non-train trips, e.g. bus, underground, foot, etc
			train_uid, date2 = train_info.split('|', 1)
			train_uid_list = [train_uid]
			while ';' in date2:
				date2, assoc_stop, train_info = date2.split(';', 2)
				assoc_uid, date2 = train_info.split('|', 1)
				train_uid_list.append(assoc_uid)
			src, dst = (cps['links'][f'/data/stations/{s}']['crs'] for s in [src, dst])
			sig_n, jst = jn_sig.trip_index(src, dst)
			sig = GWCTrip.TripSig(src, dst, train_uid_list, jst.ts_src)
			trip_order.append((sig_n, sig_key, sig))
		trip_order.sort()

		trips = list(
			GWCTrip.from_serw_cps(sig, cps['result'][sig_key], cps['links'])
			for n, sig_key, sig in trip_order )
		return cls(trips, jn_sig.ts_start, jn_sig.ts_end)

	def __init__(self, trips, ts_start, ts_end):
		self.trips, self.ts_start, self.ts_end = trips, ts_start, ts_end

	def __repr__(self):
		trips = ' - '.join(( f'{t.train_uid}'
			f'({t.ts_start.strftime("%H:%M")}+{len(t.stops)})' ) for t in self.trips)
		span = ' '.join(ts.strftime("%H:%M") for ts in [self.ts_start, self.ts_end])
		return f'<Jn [{span}] [{trips}]>'


class GWCAPISerw:

	api_tag = 'serw'

	def __init__(self, loop, conf, rate_sem=None):
		self.loop, self.conf = loop, conf
		self.rate_sem = rate_sem or RateSemaphore.dummy()
		self.log = get_logger('gwc.api.serw')
		if not self.conf.serw_api_debug: self.log.setLevel(logging.WARNING)

	async def __aenter__(self):
		self.ctx = AsyncExitStack()
		self.http = await self.ctx.enter(
			aiohttp.ClientSession(headers=self.conf.serw_http_headers) )
		return self

	async def __aexit__(self, *err):
		if not self.ctx: return
		await self.ctx.close()
		self.ctx = None

	def format_trip_diff(self, gtfs_trip, jn_trip):
		import subprocess, tempfile
		gtfs_stops, jn_stops = (list(t.stops) for t in [gtfs_trip, jn_trip])

		# Remove passing stops to highlight relevant mismatches
		jn_stops_iter, scrub = iter(enumerate(jn_stops)), list()
		for st1 in gtfs_stops:
			scrub_ext = list()
			for n, st2 in jn_stops_iter:
				if st1.crs == st2.crs:
					scrub.extend(scrub_ext)
					scrub_ext.clear()
					break
				if not st2.ts: scrub_ext.append(n)
		for n in scrub: jn_stops[n] = None
		jn_stops = list(filter(None, jn_stops))

		with tempfile.NamedTemporaryFile(prefix='.gtfs-webcheck.gtfs-trip.') as dst1,\
				tempfile.NamedTemporaryFile(prefix='.gtfs-webcheck.api-trip.') as dst2:
			for stops, dst in [(gtfs_stops, dst1), (jn_stops, dst2)]:
				for s in stops: dst.write(f'{s.crs} {s.ts}\n'.encode())
				dst.flush()
			cmd = self.conf.trip_diff_cmd.split() + [dst1.name, dst2.name]
			try:
				res = subprocess.run(cmd, stdout=subprocess.PIPE)
				if res.returncode != 1:
					raise subprocess.SubprocessError(f'exit_code={res.returncode}')
			except subprocess.SubprocessError as err:
				log_lines( self.log.error, [
					('Failed to get diff output for trips: [{}] {}', err_cls(err), err),
					('cmd: {}', cmd), ('  trip-gtfs: {}', gtfs_trip), ('  trip-api: {}', jn_trip) ])
				return
			return f'Matching journey trip [{dst2.name}]:\n  {jn_trip}\n{res.stdout.decode()}'


	def _api_cache(self, fn_tpl=None, data=..., fn=None):
		if not fn:
			if not self.conf.debug_http_dir: return
			self.conf._debug_files[fn_tpl] += 1
			fn = self.conf.debug_http_dir / fn_tpl.format(self.conf._debug_files[fn_tpl])
		if data is ...: return json.loads(fn.read_text()) if fn.exists() else None
		with fn.open('w') as dst: json.dump(data, dst, **json_pretty)

	def _api_error_check(self, http_status, data):
		if isinstance(data, dict) and data.get('errors'):
			err = data['errors']
			try:
				err, = err
				err = err.get('failureType'), err['errorCode']
			except: raise GWCAPIError(http_status, err)
			if err not in self.conf.serw_error_skip:
				err_type, err_code = err
				for chk_type, chk_code in self.conf.serw_error_skip or list():
					if chk_type != err_type: continue
					if chk_code == err_code: break
					if isinstance(chk_code, re_type) and chk_code.search(err_code): break
				else: raise GWCAPIErrorCode(http_status, *err)
		elif data and isinstance(data, dict) and 'result' not in data:
			raise GWCAPIError(http_status, f'no "result" key in data - {data!r}')
		if http_status != 200:
			raise GWCAPIError(res.status, 'non-200 response status')
		if not data: raise GWCAPIErrorEmpty(http_status, data)

	def api_url(self, p, **q):
		if not re.search('^(https?|ws):', p):
			url = f'{self.conf.serw_api_url}/{p.lstrip("/")}'
		if q:
			if '?' not in url: url += '?'
			elif not url.endswith('&'): url += '&'
			url += urllib.parse.urlencode(q)
		return url

	async def api_call(self, method, p, j=None, headers=None, q=None):
		self.log.debug('serw-api call: {} {}', method, p)

		if self.conf.debug_cache_dir:
			self.conf._debug_files['_req'] += 1
			cache_fn = 'api-cache.{:03d}.{}.{}.json'.format(
				self.conf._debug_files['_req'], method, url_to_fn(p) )
			cache_fn = self.conf.debug_cache_dir / cache_fn
			cached = self._api_cache(fn=cache_fn)
			if cached is not None:
				self.log.debug('serw-api cache-read: {}', cache_fn)
				return cached

		url = self.api_url(p, **(q or dict()))
		self._api_cache( f'api-req.{url_to_fn(p)}.res.{{:03d}}.info',
			dict(method=method, url=url, p=p, json=j, headers=list(
				f'{k}: {v}' for k,v in self.http._prepare_headers(headers).items() )) )

		try:
			async with self.http.request(method, url, json=j, headers=headers) as res:
				if res.content_type != 'application/json':
					data = await res.read()
					raise GWCAPIError(res.status, f'non-json response - {data!r}')
				data = await res.json()
				self._api_cache(f'api-req.{url_to_fn(p)}.res.{{:03d}}.json', data)
				self._api_error_check(res.status, data)

		except aiohttp.ClientError as err:
			raise GWCAPIError(None, f'[{err_cls(err)}] {err}') from None

		if self.conf.debug_cache_dir:
			self.log.debug('serw-api cache-write: {}', cache_fn)
			self._api_cache(fn=cache_fn, data=data)

		return data


	st_type = enum.Enum('StationType', [('src', 'Origin'), ('dst', 'Destination')])

	async def get_station(self, code_raw, t=None, check=False):
		code = code_raw
		if isinstance(code, int): code = f'{code:04d}'
		if code.isdigit(): code = code[:4]
		else: code = self.conf.serw_crs_nlc_map.get(code)
		if check:
			assert isinstance(code_raw, str) and len(code_raw) == 3
			res = await self.api_call(
				'get', 'config/stations', q=dict(search=code_raw, type=t.value) )
			for st_info in res['result']:
				try: st_loc = res['links'][st_info['station']]
				except KeyError: continue
				nlc, crs = map(str, [st_loc['nlc'], st_loc['crs']])
				if code_raw in [crs, nlc] or code in [crs, nlc]: return nlc
		elif code and len(code) == 4: return code
		raise GWCError(f'Falied to process station code to 4-digit nlc: {code_raw!r}')

	async def get_journeys(self, src, dst, ts_dep=None):
		'''Query API and return a list of journeys from src to dst,
				starting at ts_dep UTC/BST datetime (without timezone, default: now) or later.
			ts_dep can be either datetime or tuple to specify departure date/time range.
			Default ts_dep range if only one datetime is specified is (ts_dep, ts_dep+3h).
			Returns None if this query cannot be performed, e.g. due to missing src/dst in API.'''

		# Default is to use current time as ts_start and +3h as ts_end
		if not ts_dep:
			ts = dt.datetime.utcnow()
			if ( (ts.month > 3 or ts.month < 10) # "mostly correct" (tm) DST hack for BST
					or (ts.month == 3 and ts.day >= 27)
					or (ts.month == 10 and ts_start.day <= 27) ):
				ts += dt.timedelta(seconds=3600)
			ts_dep = ts
		if not isinstance(ts_dep, tuple):
			ts_dep = ts_dep, ts_dep + dt.timedelta(seconds=3*3600)
		ts_start, ts_end = (( ts if isinstance(ts, str)
			else ts.strftime('%Y-%m-%dT%H:%M:%S') ) for ts in ts_dep )

		try:
			jp_res = await self.api_call( 'post', 'jp/journey-plan',
				dict( origin=src, destination=dst,
					outward=dict(rangeStart=ts_start, rangeEnd=ts_end, arriveDepart='Depart'),
					numJourneys=self.conf.test_trip_journeys, adults=1, children=0,
					openReturn=False, disableGroupSavings=True, showCheapest=False, doRealTime=False ) )
		except GWCAPIErrorEmpty: return None

		jp_urls = list(
			urllib.parse.unquote_plus(res['journey'])
			for res in jp_res['result']['outward'] )

		journeys = list()
		for jp_url in jp_urls:
			jn_sig = GWCJnSig.from_serw_url(jp_url.rsplit('/', 1)[-1])
			cps = await self.api_call('get', f'{jp_url}/calling-points')
			journeys.append(GWCJn.from_serw_cps(jn_sig, cps))
		return journeys


	def ready(self):
		return not self.rate_sem.locked()

	async def test_trip(self, trip):
		fail = self.conf.debug_trigger_mismatch
		fail = fail.lower().split() if fail else list()

		async with self.rate_sem:
			# Try fast query without checking whether stops are valid for API
			src, dst = (trip.stops[n].crs for n in [0, -1])
			try:
				src_nlc = await self.get_station(src, self.st_type.src)
				dst_nlc = await self.get_station(dst, self.st_type.dst)
				if src_nlc == dst_nlc: raise GWCError
			except GWCError as err: jns = None
			else:
				jns = await self.get_journeys(
					src_nlc, dst_nlc, ts_dep=(trip.ts_start, trip.ts_end) )

			if jns is None:
				# Slower query, picking valid stops first, then looking for journeys again
				ends = dict.fromkeys(['src', 'dst'])
				stops = list(enumerate(map(op.attrgetter('crs'), trip.stops)))
				n_chk, stops = -1, dict(src=iter(stops), dst=iter(reversed(stops)))
				while not all(ends.values()):
					for k in ends.keys():
						if ends[k]: continue
						n, crs = next(stops[k])
						if n == n_chk: break # src=dst
						n_chk = max(n_chk, n)
						try: nlc = await self.get_station(crs, self.st_type[k], check=True)
						except GWCError as err: continue
						ends[k] = crs, nlc
						if ends['src'] == ends['dst']: break
					else: continue
					raise GWCTestSkipTrip(self.api_tag, 'trip has no api-valid stops')
				(src, src_nlc), (dst, dst_nlc) = ends['src'], ends['dst']
				self.log.warning( 'Limiting check to [{} {}]'
					' segment due to api limitations for trip: {}', src, dst, trip )
				jns = await self.get_journeys(src_nlc, dst_nlc, ts_dep=(trip.ts_start, trip.ts_end))

		if jns is None: raise GWCTestSkipTrip(self.api_tag, 'api lookup fails')
		log_lines(self.log.debug, [ 'Returned journeys',
			*textwrap.indent(pformat_data(jns), '  ').splitlines() ])

		## Find one-direct-trip journey with matching train_uid
		# Failing that, try to get trip with one of the trains of the association
		jns_dict = dict((jn.trips[0].train_uid, jn) for jn in jns if len(jn.trips) == 1)
		for train_uid in [trip.train_uid, *trip.train_uid.split('_')]:
			if train_uid not in jns_dict: continue
			jn_trip = jns_dict[train_uid].trips[0]
			if 'nojourney' in fail:
				jn_trip.train_uid += 'x'
				continue
			break
		else: raise GWCTestFailNoJourney(self.api_tag, trip, jns)

		## Match all stops/stop-times
		# SERW API returns non-public stops (often duplicated), which are missing in gtfs
		# Check is restricted to [src, dst] interval, can be subset of trip.stops
		jn_stops_iter, mismatch_n = iter(jn_trip.stops), random.randrange(0, len(trip.stops))
		for n, st1 in it.dropwhile(lambda t: t[1].crs != src, enumerate(trip.stops)):
			if n == mismatch_n and 'stopnotfound' in fail: st1.crs += 'x'
			for st2 in jn_stops_iter:
				if not st2.ts: continue # possible non-public duplicate before public one
				if st1.crs == st2.crs: break
				if st2.ts:
					raise GWCTestFailStopNotFound(
						self.api_tag, trip, [jn_trip, st2], diff=self.format_trip_diff(trip, jn_trip) )
			else:
				raise GWCTestFailStopNotFound(
					self.api_tag, trip, [jn_trip, st1], diff=self.format_trip_diff(trip, jn_trip) )
			if n == mismatch_n and 'stopmismatch' in fail:
				st1.ts = st2.ts + dt.timedelta(seconds=self.conf.test_trip_time_slack + 5*60)
			ts1, ts2 = ((0 if not st.ts else st.ts.total_seconds()) for st in [st1, st2])
			ts_diff = abs(ts1 - ts2)
			if ts_diff > self.conf.test_trip_time_slack:
				raise GWCTestFailStopMismatch( self.api_tag, trip,
					diff=self.format_trip_diff(trip, jn_trip),
					data=[jn_trip, st1.crs, (st1.ts, st2.ts), (ts_diff, self.conf.test_trip_time_slack)] )
			if st1.crs == dst: break


class GWCTestRunner:
	'Fetch trips from mysql at random and test them against specified APIs.'

	def __init__(self, loop, conf, api_list):
		self.loop, self.conf = loop, conf
		self.log, self.log_diffs = get_logger('gwc.test'), get_logger('gwc.diffs')
		self.api_list = list(api_list)

	async def __aenter__(self):
		self.ctx = AsyncExitStack()

		# Reset locale for consistency in calendar and such
		locale_prev = locale.setlocale(locale.LC_ALL, '')
		self.ctx.callback(locale.setlocale, locale.LC_ALL, locale_prev)

		# Warnings from aiomysql about buffered results and such are all bugs
		await self.ctx.enter(warnings.catch_warnings())
		warnings.filterwarnings('error')

		self.trip_log, self.trip_skip = None, set()
		if self.conf.test_trip_log:
			self.trip_log = await self.ctx.enter(self.conf.test_trip_log.open('a+'))
			self.trip_log.seek(0)
			self.trip_skip.update(map(int, filter(str.isdigit, self.trip_log.read().split())))
			self.log.debug(
				'Using trip_id skip-list ({} item[s]) from: {}',
				len(self.trip_skip), self.trip_log.name )

		self.log.debug('Connecting to mysql...') # reminder in case it hangs due to network
		self.db_conns, self.db_cursors = dict(), dict()
		self.db_pool = await self.ctx.enter(
			aiomysql.create_pool(charset='utf8mb4', **(self.conf.mysql_conn_opts or dict())) )
		self.c = await self.connect()
		self.db = self.db_conns[None]

		for n, api_ctx in enumerate(self.api_list):
			self.api_list[n] = await self.ctx.enter(api_ctx)
		return self

	async def __aexit__(self, *err):
		if self.ctx: self.ctx = await self.ctx.close()
		self.db_conns = self.db_cursors = self.db = self.c = None


	async def connect(self, key=None):
		assert key not in self.db_conns, key
		conn = self.db_conns[key] = await self.ctx.enter(self.db_pool.acquire())
		c = self.db_cursors[key] = await self.ctx.enter(conn.cursor(NTCursor))
		await c.execute('show variables like %s', ['sql_mode'])
		mode_flags = set(map( str.strip,
			dict(await c.fetchall())['sql_mode'].lower().split(',') ))
		mode_flags.update(self.conf.mysql_sql_mode.lower().split())
		await c.execute('set sql_mode = %s', [','.join(mode_flags)])
		await c.execute(f'use {self.conf.mysql_db_name}')
		return c

	async def q(self, q, *params, c='iter'):
		c = self.db_cursors.get(c) or await self.connect(c)
		await c.execute(q, params)
		async for row in c: yield row

	async def qb(self, q, *params, c=None, flat=False, **kws):
		c = self.c if not c else (self.db_cursors.get(c) or await self.connect(c))
		await c.execute(q, params)
		data = await c.fetchall()
		if flat: data = list(map(op.itemgetter(0), data))
		return data

	def escape(self, val):
		return self.db.escape(val)


	async def _check_trip(self, trip, api_list, max_parallel):
		'''Runs trip check on random apis with specified
			max concurrency, preferring ones that are ready first.'''
		pending, api_list = list(), list(filter(None, api_list))
		try:
			random.shuffle(api_list)
			while api_list or pending:
				if api_list and len(pending) < max_parallel:
					api_list.sort(key=op.methodcaller('ready'))
					while len(pending) < max_parallel:
						pending.append(self.loop.create_task(api_list.pop().test_trip(trip)))
				done, pending = await asyncio.wait(
					pending, return_when=asyncio.FIRST_COMPLETED )
				for fut in done:
					exc = fut.exception()
					yield GWCTestResult(not exc, exc)
		finally:
			for task in pending:
				task.cancel()
				await task

	async def check_trip(self, trip, api_list=None, ts_start=None):
		'''Checks trip info against API, passing
			raised GWCTestFail exception(s) wrapped into GWCTestBatchFail.'''
		if api_list is None: api_list = self.api_list
		if ts_start: trip = trip.copy(ts_start=ts_start)
		tm, tm_parallel = self.conf.test_match, self.conf.test_match_parallel
		if tm is tm.any: tm_parallel = 1
		test_result_iter = self._check_trip(trip, api_list, tm_parallel)
		if tm is tm.all:
			async for res in test_result_iter:
				if not res.success: raise GWCTestBatchFail(res.exc)
		elif tm is tm.any:
			async for res in test_result_iter:
				if not res.success: raise GWCTestBatchFail(res.exc)
				break
		elif tm is tm.first:
			exc_list = list()
			async for res in test_result_iter:
				if res.success: break
				exc_list.append(res.exc)
			else: raise GWCTestBatchFail(*exc_list)
		else: raise ValueError(tm)

	def trip_log_update(self, t, result=None):
		if not self.trip_log: return
		line = f'{t.trip_id} uid={t.trip_headsign}'
		if result: line += f' {result}'
		self.trip_log.write(f'{line}\n')
		self.trip_log.flush()


	async def _pick_trips(self, weights=None, pick_uids=None):
		weights = weights or self.conf.test_pick_trip or dict(seq=1)
		pick_trip_order = 'RAND()' if self.conf.test_pick_trip_random_order else 'trip_id'

		trip_id_skip = ( '1' if not self.trip_skip else
			f'trip_id NOT IN ({",".join(map(self.escape, self.trip_skip))})' )
		pick_checks = dict( seq='1',
			assoc=r"trip_headsign LIKE '%%\_%%'",
			z="trip_headsign LIKE 'Z%%'" )
		if set(weights).difference(pick_checks): raise ValueError(weights)
		for k in set(pick_checks).difference(weights): del pick_checks[k]

		train_uid_check = '1'
		if isinstance(pick_uids, int):
			train_uid_checks = dict()
			for k,c in pick_checks.items():
				train_uid_set = set(await self.qb(f'''
					SELECT trip_headsign FROM trips
					WHERE {c} GROUP BY trip_headsign
					ORDER BY {pick_trip_order} LIMIT {pick_uids}''', flat=True))
				train_uid_checks[k] = train_uid_set
			train_uid_set, n = set(), sum(map(len, train_uid_checks.values()))
			while len(train_uid_set) < pick_uids and n > 0:
				pick = random_weight(weights, train_uid_checks)
				try: train_uid_set.add(train_uid_checks[pick].pop())
				except KeyError: continue
				n -= 1
			train_uid_check = ( '0' if not train_uid_set else
				f'trip_headsign IN ({",".join(map(self.escape, train_uid_set))})' )
		elif pick_uids:
			train_uid_check = ( 'trip_headsign IN '
				f'({",".join(map(self.escape, set(pick_uids)))})' )

		trip_count = (await self.qb(
			'SELECT COUNT(DISTINCT trip_id) FROM ({}) u'.format(
				' UNION '.join( f'''( SELECT trip_id FROM trips
					WHERE {c} AND {trip_id_skip} AND {train_uid_check} )'''
				for k,c in pick_checks.items() )) ))[0][0]
		yield trip_count

		q_base = f'''
			SELECT %s AS t, t.* FROM trips t WHERE {{c}} AND
			{trip_id_skip} AND {train_uid_check} ORDER BY {pick_trip_order}'''
		trip_iters = dict(
			(k, self.q(q_base.format(c=c), k, c=f'trips_{k}'))
			for k,c in pick_checks.items() )

		while trip_iters:
			pick = random_weight(weights, trip_iters)
			try: t = await anext(trip_iters[pick])
			except AsyncStopIteration:
				del trip_iters[pick]
				continue
			stops = await self.qb( 'SELECT * FROM'
				' stop_times WHERE trip_id = %s', t.trip_id )
			if stops: yield t, stops

	async def pick_trips(self):
		self.stats['trip-skip-set-init'] = len(self.trip_skip)
		trips = self._pick_trips(pick_uids=self.conf.test_train_uids)
		yield (await anext(trips)) # trip count
		async for t, stops in trips:
			# Same trip_id can be yielded by
			#  different iterators, hence additional check here
			if t.trip_id in self.trip_skip: continue
			yield t, stops
			self.trip_skip.add(t.trip_id)

	def pick_dates(self, dates, weights=None, current=None):
		'Pick dates to test according to weights in conf.test_pick_date.'
		if not current: current = dt.date.today()
		dates, weights = list(dates), weights or self.conf.test_pick_date or dict(seq=1)
		if not dates: return dates
		dates_pick, week_pos = list(), bisect.bisect_left(dates, current + dt.timedelta(7))
		pick_seq = dict(
			seq=iter(dates),
			seq_next_week=iter(dates[:week_pos]),
			seq_after_next_week=iter(dates[week_pos:]) )
		dates_holidays = (self.conf.bank_holidays or set()).intersection(dates)
		weights = dict((k,v) for k,v in weights.items() if v > 0)
		while weights and len(dates_pick) < min(len(dates), self.conf.test_trip_dates):
			pick = random_weight(weights)
			if pick in pick_seq:
				for date in pick_seq[pick]:
					if date not in dates_pick:
						dates_pick.append(date)
						break
				else: del weights[pick]
			elif pick == 'bank_holiday':
				date = None
				while dates_holidays:
					date = dates_holidays.pop()
					if date not in dates_pick: break
				if date: dates_pick.append(date)
				else: del weights[pick]
			elif pick == 'random':
				dates_left = list(set(dates).difference(dates_pick))
				if dates_left: dates_pick.append(random.choice(dates_left))
				else: del weights[pick]
			else: raise ValueError(pick)
		return dates_pick

	def pick_dates_holidays(self, dates):
		return self.pick_dates(dates, weights=dict(bank_holiday=1))


	async def run(self):
		self.stats = collections.Counter()
		self.stats['diff-total'] = 0

		pick_funcs = pick_funcs_base = 'pick_trips', 'pick_dates'
		if self.conf.test_pick_special:
			pick_funcs = (f'{v}_{self.conf.test_pick_special}' for v in pick_funcs)
		pick_trips, pick_dates = (
			getattr(self, k, getattr(self, k0))
			for k,k0 in zip(pick_funcs, pick_funcs_base) )

		trips = pick_trips()
		trip_count = await anext(trips)

		self.stats['trip-count'] = trip_count
		trip_count_skip = '' if not self.trip_skip else f' ({len(self.trip_skip)} in skip-list)'
		self.log.debug('Checking {} trip(s){}...', trip_count, trip_count_skip)
		progress = progress_iter(self.log, 'trips', trip_count)

		async for t, stops in trips:
			next(progress)
			ts = dt.datetime.now()
			date_current, time_current = ts.date(), ts.time()
			trip_id, train_uid, service_id = t.trip_id, t.trip_headsign, t.service_id
			log_trip = LogPrefixAdapter(self.log, f'{train_uid}.{trip_id}.{service_id}')

			# Find first/last public pickup/dropoff stops for a trip
			test_stops, buff = list(), list()
			for s in stops:
				if not test_stops:
					if s.pickup_type == GTFSEmbarkType.none: continue
					test_stops.append(s)
				else:
					buff.append(s)
					if s.drop_off_type == GTFSEmbarkType.none: continue
					test_stops.extend(buff)
					buff.clear()
			if len(test_stops) >= 2 and test_stops[0].stop_id == test_stops[-1].stop_id:
				test_stops = test_stops[:-1] # to avoid OriginDestinationSame error
			if len(test_stops) < 2:
				self.stats['trip-skip-no-public-stops'] += 1
				continue
			time0 = dts_to_dt(test_stops[0].departure_time)
			trip = GWCTrip.from_gtfs_stops(train_uid, test_stops)

			# Build list of future service dates for a trip, limited by test_trip_days
			dates = await self.qb(f'''
				SELECT
					service_id AS id, start_date AS a, end_date AS b,
					CONCAT(monday, tuesday, wednesday, thursday, friday, saturday, sunday) AS weekdays,
					date, exception_type AS exc
				FROM calendar c
				LEFT JOIN calendar_dates cd USING(service_id)
				WHERE service_id = %s''', service_id)
			svc = dates[0]
			dates = list() if svc.date is None else [svc, *dates]
			dates, exc_dates = (
				set(row.date for row in dates if row.exc == GTFSExceptionType.added),
				set(row.date for row in dates if row.exc == GTFSExceptionType.removed) )
			span = Timespan(svc.a, svc.b, tuple(map(int, svc.weekdays)), exc_dates)
			dates.update(span.date_iter())
			dates = list(it.dropwhile(lambda date: not ( date > date_current
				or (date == date_current and time0 > time_current) ), sorted(dates)))
			dates = dates[:bisect.bisect_left( dates,
				dt.date.today() + dt.timedelta(self.conf.date_max_future_offset) )]
			if self.conf.test_pick_date_set:
				dates = sorted(set(self.conf.test_pick_date_set).intersection(dates))
			dates = pick_dates(dates, current=date_current)
			if not dates:
				# log_trip.debug('no valid dates to check, skipping')
				self.stats['trip-skip-dates'] += 1
				self.trip_log_update(t, '-skip-')
				continue

			# Check produced trip info against API(s)
			self.stats['trip-check'] += 1
			trip_diffs, trip_skip, api_list = list(), False, list(self.api_list)
			for date in dates:
				log_trip.debug('[{}] checking date...', date)
				ts_src = dt.datetime.combine(date, time0) - self.conf.test_trip_embark_delay
				self.stats['trip-check-date'] += 1
				try:
					await self.check_trip(trip, api_list, ts_start=ts_src)
					log_trip.debug('[{}] result: match', date)
				except GWCTestBatchFail as err_batch:
					trip_date_diffs = list()
					for err in err_batch.exc_list:
						err_type = err_cls(err)

						if isinstance(err, GWCTestSkipTrip):
							log_trip.debug('trip-check impossible using api [{}] - {}', err.api, err)
							self.stats['trip-skip-api'] += 1
							api_list = list(filter(lambda api: api.api_tag != err.api, api_list))
							if not api_list: trip_skip = True
							continue
						elif isinstance(err, GWCTestSkip):
							log_trip.debug('check skipped due to api [{}] limitation - {}', err.api, err)
							self.stats['trip-skip-api-date'] += 1
							continue # not actually a diff

						elif not isinstance(err, GWCTestFail):
							try: raise err from None
							except Exception as err:
								log_lines(
									self.log.error, log_func_last=self.log.exception,
									lines=[ 'Unexpected error during trip check [BUG]',
										('  trip: {}', trip), ('  error: [{}] {}', err_type, err) ])
							err_api, err_type, err = 'core', 'bug', None

						else: err_api = err.api

						trip_date_diffs.append(f'{err_api}.{err_type}')
						self.stats[f'diff-api-{err_api}'] += 1
						self.stats[f'diff-type-{err_type}'] += 1
						if err:
							err_info = [
								('API [{}] data mismatch for gtfs trip: {}', err_api, err_type),
								('Trip: {}', trip), ('Date/time: {}', ts_src) ]
							if err.diff or err.data:
								err_info.append('Diff details:')
								err_info.extend(textwrap.indent(
									err.diff or pformat_data(err.data), '  ' ).splitlines())
							log_lines(self.log_diffs.error, err_info)

					trip_diffs.extend(trip_date_diffs)
					log_trip.debug( '[{}] result: {}', date,
						', '.join(trip_date_diffs) if trip_date_diffs else f'skip (trip={trip_skip})' )
				if trip_skip: break
			self.stats['diff-total'] += len(trip_diffs)

			if self.trip_log:
				if trip_skip: trip_res = '-skip-'
				else: trip_res = '-match-' if not trip_diffs else ':'.join(sorted(set(trip_diffs)))
				self.trip_log_update(t, trip_res)

		log_lines(self.log.debug, ['Stats:', *(
			'  {{}}: {}'.format('{:,}' if isinstance(v, int) else '{:.1f}').format(k, v)
			for k,v in sorted(self.stats.items()) )])
		if self.stats['diff-total'] > 0: raise GWCTestFoundDiffs(self.stats['diff-total'])


async def run_tests(loop, conf):
	exit_code, log = 1, get_logger('gwc.tests')
	task = asyncio.Task.current_task(loop)
	def sig_handler(sig, code):
		log.info('Exiting on {} signal with code: {}', sig, code)
		nonlocal exit_code
		exit_code = code
		task.cancel()
	for sig, code in ('INT', 1), ('TERM', 0):
		loop.add_signal_handler(getattr(signal, f'SIG{sig}'), ft.partial(sig_handler, sig, code))
	api_rate_sem = RateSemaphore( conf.rate_interval,
		conf.rate_min_seq_delay, conf.rate_max_concurrency )
	api_list = [GWCAPISerw(loop, conf, api_rate_sem)]
	async with GWCTestRunner(loop, conf, api_list) as tester:
		task = loop.create_task(tester.run())
		try: await task
		except asyncio.CancelledError as err: pass
		except GWCTestFoundDiffs: exit_code = 174 # LSB Init Script Actions: 150-199
		else: exit_code = 0
	return exit_code


def main(args=None, conf=None):
	if not conf: conf = TestConfig()

	import argparse
	parser = argparse.ArgumentParser(
		description='Tool to test gtfs feed (stored in mysql) against online data sources.')

	group = parser.add_argument_group('Configuration')
	group.add_argument('-c', '--conf', metavar='file',
		help='YAML configuration file to read, overriding any defaults with values there.'
			' Requires "pyyaml" module. See doc/gtfs-webcheck.example.yaml for example.')

	group = parser.add_argument_group('Testing options')
	group.add_argument('-s', '--trip-id-log', metavar='file',
		help='Append each checked trip_id to specified file, and skip ones that are already there.')
	group.add_argument('-f', '--diff-log', metavar='file',
		help='Log diffs to a specified file'
				' (using WatchedFileHandler) instead of stderr that default logging uses.'
			' "-" or "1" can be used for stdout, any integer value for other open fds.')
	group.add_argument('--diff-log-fmt', metavar='format',
			help='Log line format for --diff-log for python stdlib logging module.')
	group.add_argument('-n', '--test-train-limit', type=int, metavar='n',
		help='Randomly pick specified number of distinct train_uids for testing, ignoring all others.')
	group.add_argument('-u', '--test-train-uid', metavar='uid-list',
		help='Test trips for specified train_uid only. Multiple values are split by spaces.')
	group.add_argument('-t', '--test-date', metavar='date-list',
		help='Only test specified dates (iso8601 format), skipping'
			' trips that dont run on them. Multiple values are split by spaces.')
	group.add_argument('--test-special',
		metavar='name', choices=conf.test_pick_special_iters,
		help='Use special named trip/date selection iterators. Choices: %(choices)s')

	group = parser.add_argument_group('MySQL db parameters')
	group.add_argument('-d', '--gtfs-db-name', metavar='db-name',
		help=f'Database name to read GTFS data from (default: {conf.mysql_db_name}).')
	group.add_argument('--mycnf-file', metavar='file',
		help='Alternative ~/.my.cnf file to use to read all connection parameters from.'
			' Parameters there can include: host, port, user, passwd, connect,_timeout.'
			' Overidden parameters:'
				' db (specified via --gtfs-db-name option),'
				' charset=utf8mb4 (for max compatibility).')
	group.add_argument('--mycnf-group', metavar='group',
		help='Name of "[group]" (ini section) in ~/.my.cnf ini file to use parameters from.')

	group = parser.add_argument_group('Extra data sources')
	group.add_argument('--bank-holiday-list',
		metavar='file', default='doc/UK-bank-holidays.csv',
		help='List of dates, one per line, for bank holidays, used only'
			' for testing priorities, "-" or empty value to disable. Default: %(default)s (if exists).')
	group.add_argument('--bank-holiday-fmt',
		metavar='strptime-format', default='%d-%b-%Y',
		help='strptime() format for each line in --bank-holiday-list file. Default: %(default)s')
	group.add_argument('--serw-crs-nlc-csv',
		metavar='file', default='doc/UK-stations-crs-nlc.csv',
		help='UK crs-to-nlc station code mapping table ("crs,nlc" csv file).'
			' Either of these codes can be used for data lookups.'
			' Empty value or "-" will create empty mapping. Default: %(default)s')

	group = parser.add_argument_group('Misc dev/debug options')
	group.add_argument('--debug-cache-dir', metavar='file',
		help='Cache API requests to dir if missing, or re-use cached ones from there.')
	group.add_argument('--debug-http-dir', metavar='file',
		help='Directory path to dump various http request/response info to.')
	group.add_argument('-x', '--debug-trigger-mismatch', metavar='type',
		help='Trigger data mismatch of specified type in all tested entries.'
			' Supported types correspond to implemented GWCTestFail'
				' exceptions, e.g.: NoJourney, StopNotFound, StopMismatch.'
			' Multiple values can be specified in one space-separated arg.')
	group.add_argument('--debug-rng-seed', help='Random number generator seed.')
	group.add_argument('--debug', action='store_true', help='Verbose operation mode.')

	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	if opts.conf: conf._update_from_file(pathlib.Path(opts.conf))

	sys.stdout = open(sys.stdout.fileno(), 'w', 1)
	logging.basicConfig(
		datefmt='%Y-%m-%d %H:%M:%S',
		format='%(asctime)s :: {}%(levelname)s :: %(message)s'\
			.format('%(name)s ' if opts.debug else ''),
		level=logging.DEBUG if opts.debug else logging.INFO )
	log = get_logger('gwc.main')

	if opts.trip_id_log: conf.test_trip_log = pathlib.Path(opts.trip_id_log)

	if opts.diff_log:
		if opts.diff_log == '-': opts.diff_log = '1'
		handler = (
			logging.StreamHandler(open(int(opts.diff_log), 'w', 1))
			if opts.diff_log.isdigit() else logging.handlers.WatchedFileHandler(opts.diff_log) )
		if opts.diff_log_fmt: handler.setFormatter(logging.Formatter(opts.diff_log_fmt))
		handler.setLevel(0)
		logger = get_logger('gwc.diffs')
		logger.setLevel(0)
		logger.addHandler(handler)

	if opts.serw_crs_nlc_csv and opts.serw_crs_nlc_csv != '-':
		crs_nlc_map, lines_warn = dict(), list()
		with open(opts.serw_crs_nlc_csv) as src:
			for n, line in enumerate(src, 1):
				try: crs, nlc = line.strip().split(',',1)
				except ValueError: pass
				else:
					if len(crs) == 3 and len(nlc) in [4, 6]:
						if len(nlc) == 6 and not nlc.endswith('00'): continue
						crs_nlc_map[crs] = nlc[:4]
						continue
				lines_warn.append((n, line))
			if len(lines_warn) > 20:
				n, line = lines_warn[0]
				log.warning( 'Failed to process {} "crs,nlc"'
					' csv lines, first one: {!r} [{}]', len(lines_warn), line, n )
			elif lines_warn:
				for n, line in lines_warn:
					log.warning('Failed to process "crs,nlc" csv line: {!r} [{}]', line, n)
		conf.serw_crs_nlc_map = crs_nlc_map

	if opts.bank_holiday_list and opts.bank_holiday_list != '-':
		conf.bank_holidays = set()
		p = pathlib.Path(opts.bank_holiday_list)
		if not p.exists():
			log.warning('Missing bank holiday list file (use "-" or empty to disable): {}', p)
		else:
			with p.open() as src:
				for line in src.read().splitlines():
					conf.bank_holidays.add(dt.datetime.strptime(line, opts.bank_holiday_fmt).date())

	mycnf_path = opts.mycnf_file or str(pathlib.Path('~/.my.cnf').expanduser())
	conf.mysql_conn_opts = dict(filter(op.itemgetter(1), dict(
		read_default_file=mycnf_path, read_default_group=opts.mycnf_group ).items()))
	if opts.gtfs_db_name: conf.mysql_db_name = opts.gtfs_db_name

	if opts.test_train_uid:
		conf.test_train_uids = opts.test_train_uid.split()
		if opts.test_train_limit:
			conf.test_train_uids = conf.test_train_uids[:opts.test_train_limit]
	elif opts.test_train_limit: conf.test_train_uids = opts.test_train_limit

	if opts.test_special:
		conf.test_pick_special = conf.test_pick_special_iters[opts.test_special]
	if opts.test_date:
		conf.test_pick_date_set = set(
			dt.date(*map(int, d.split('-', 2))) for d in opts.test_date.split() )

	if opts.debug_http_dir:
		conf.debug_http_dir = pathlib.Path(opts.debug_http_dir)
		conf.debug_http_dir.mkdir(parents=True, exist_ok=True)
	if opts.debug_cache_dir:
		conf.debug_cache_dir = pathlib.Path(opts.debug_cache_dir)
		conf.debug_cache_dir.mkdir(parents=True, exist_ok=True)
	if opts.debug_trigger_mismatch: conf.debug_trigger_mismatch = opts.debug_trigger_mismatch
	if opts.debug_rng_seed: random.seed(opts.debug_rng_seed)

	log.debug('Starting run_tests loop...')
	with contextlib.closing(asyncio.get_event_loop()) as loop:
		try: exit_code = loop.run_until_complete(run_tests(loop, conf))
		except asyncio.CancelledError as err: exit_code = 1
	log.debug('Finished')
	return exit_code

if __name__ == '__main__': sys.exit(main())
