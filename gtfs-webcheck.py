#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
import datetime as dt
import os, sys, pathlib, signal, locale, warnings
import contextlib, inspect, collections, enum, time
import asyncio, urllib.parse, json, re, random, secrets
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

	## test_pick_*: weights for picking which trips/days to test first.
	# "seq" in all of these is a simple sequential pick.
	test_pick_trip = dict(seq=1, assoc=0.5, z=0.2)
	test_pick_date = dict(seq=1, bank_holiday=2, random=2)

	## test_train_uids: either integer to pick n random train_uids or list of specific uids to use.
	test_train_uids = None

	## test_trip_log: path to file to append tested trip_id's to and skip ones already there.
	test_trip_log = None

	# XXX: negative tests - specifically pick bank holidays and exception days
	test_trip_dates = 3
	test_trip_embark_delay = dt.timedelta(seconds=20*60)
	test_trip_journeys = 3 # should be high enough for testee direct trip to be there
	test_trip_time_slack = 5*60 # max diff in stop times

	trip_diff_cmd = 'diff -uw' # for pretty-printing diffs between trip stops
	bank_holidays = None

	mysql_db_name = None
	mysql_conn_opts = None
	mysql_sql_mode = 'strict_all_tables'

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
	serw_error_skip = {
		# Skip any IPTIS-related warnings - should only be relevant for fares afaict
		('Warning', 'IptisNrsError'), ('Warning', 'IptisNoRealTimeDataAvailable') }

	rate_interval = 3 # seconds
	rate_max_concurrency = 1
	rate_min_seq_delay = 0 # seconds, only if rate_max_concurrency=1

	debug_http_dir = None
	debug_cache_dir = None
	debug_trigger_mismatch = None

	def __init__(self):
		self._debug_files = collections.Counter()


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

def random_weight(items, keys_subset=None):
	if isinstance(items, dict): items = items.items()
	keys = set(keys_subset or set())
	keys, weights = zip(*((k,v) for k,v in items if not keys or k in keys))
	return random.choices(keys, weights)[0]

popn = lambda v,n: list(v.pop() for m in range(n))
url_to_fn = lambda p: p.replace('/', '-').replace('.', '_')
json_pretty = dict(sort_keys=True, indent=2, separators=(',', ': '))
pformat_data = lambda data: pprint.pformat(data, indent=2, width=100)

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
			except asyncio.CancelledError: self.queue.release()

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
	if isinstance(dts, dt.timedelta): dts = dts.total_seconds()
	if isinstance(dts, dt.datetime):
		dts = dts.time()
		dts = dts.hour * 3600 + dts.minute * 60 + dts.second
	dts = int(dts)
	ts = dt.time(dts // 3600, (dts % 3600) // 60, dts % 60, dts % 1)
	if date: ts = dt.datetime.combine(date, ts)
	return ts


class GWCError(Exception): pass
class GWCAPIError(GWCError): pass
class GWCAPIErrorCode(GWCAPIError): pass

class GWCTestFoundDiffs(Exception): pass

class GWCTestBatchFail(Exception):
	def __init__(self, *exc_list): self.exc_list = exc_list

class GWCTestFail(Exception):
	def __init__(self, api, trip, data, diff=None):
		self.api, self.trip, self.diff, self.data = api, trip, diff, data

class GWCTestFailNoJourney(GWCTestFail): pass
class GWCTestFailStopNotFound(GWCTestFail): pass
class GWCTestFailStopMismatch(GWCTestFail): pass

GWCTestResult = collections.namedtuple('GWCTestResult', 'success exc')


class GWCTripStop:

	def __init__(self, crs, ts, pickup, dropoff, **meta):
		self.crs, self.ts, self.pickup, self.dropoff, self.meta = crs, ts, pickup, dropoff, meta

	def __repr__(self):
		embark = '-P'[bool(self.pickup)] + '-D'[bool(self.dropoff)]
		ts = str(self.ts) if self.ts else 'x'
		return f'<TS {self.crs} {self.meta.get("nlc", "x")} {embark} {ts}>'

class GWCTrip:

	TripSig = collections.namedtuple('TripSig', 'src dst train_uid ts_src')

	@classmethod
	def from_serw_cps(cls, sig, stops, links):
		embark_flags = dict(
			Normal=(True, True), Passing=(False, False),
			PickUpOnly=(True, False), SetDownOnly=(False, True) )
		src, dst, trip_stops, ts0 = sig.src, sig.dst, list(), None
		for stop in stops:
			stop_info = links[stop['station']]
			pickup, dropoff = embark_flags[stop['pattern']]
			ts = ( None if not (pickup or dropoff) else
				dt.datetime.strptime(stop['time']['scheduledTime'], '%Y-%m-%dT%H:%M:%S') )
			if ts:
				if not ts0: ts0 = dt.datetime(ts.year, ts.month, ts.day)
				ts -= ts0
			name, crs, nlc, lat, lon = op.itemgetter(
				'name', 'crs', 'nlc', 'latitude', 'longitude' )(stop_info)
			if src and src != crs: continue
			src = trip_stops.append(GWCTripStop(
				crs, ts, pickup, dropoff, name=name, nlc=nlc, lat=lat, lon=lon ))
			if dst == crs: break
		return cls(sig.train_uid, trip_stops, sig.ts_src)

	@classmethod
	def from_gtfs_stops(cls, train_uid, stops, ts_src=None):
		trip_stops = list(GWCTripStop( s.stop_id,
			s.departure_time, s.pickup_type, s.drop_off_type ) for s in stops)
		return cls(train_uid, trip_stops, ts_src)

	def __init__(self, train_uid, stops, ts_start=None, ts_end=None):
		self.train_uid, self.stops = train_uid, stops
		if stops and (ts_start or ts_end):
			if not ts_start:
				ts_start = dts_to_dt(stops[0].ts, ts_end.date())
				while ts_start >= ts_end: ts_start -= dt.timedelta(days=1)
			if not ts_end:
				ts_end = dts_to_dt(stops[-1].ts, ts_start.date())
				while ts_end <= ts_start: ts_end += dt.timedelta(days=1)
		self.ts_start, self.ts_end = ts_start, ts_end

	def __repr__(self):
		return (
			f'<Trip {self.train_uid}'
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
			# XXX: not sure what date1/date2 represent here
			src, dst, date1, train_info = sig_key.split(';')
			if not train_info: continue
			train_uid, date2 = train_info.split('|', 1)
			src, dst = (cps['links'][f'/data/stations/{s}']['crs'] for s in [src, dst])
			sig_n, jst = jn_sig.trip_index(src, dst)
			sig = GWCTrip.TripSig(src, dst, train_uid, jst.ts_src)
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
					('Failed to get diff output for trips: [{}] {}',
						err.__class__.__name__, err ),
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
				raise GWCAPIErrorCode(http_status, *err)
		elif isinstance(data, dict) and 'result' not in data:
			raise GWCAPIError(http_status, f'no "result" key in data - {data!r}')
		if http_status != 200:
			raise GWCAPIError(res.status, 'non-200 response status')

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
			raise GWCAPIError(None, f'[{err.__class__.__name__}] {err}') from None

		if self.conf.debug_cache_dir:
			self.log.debug('serw-api cache-write: {}', cache_fn)
			self._api_cache(fn=cache_fn, data=data)

		return data


	st_type = enum.Enum('StationType', [('src', 'Origin'), ('dst', 'Destination')])

	async def get_station(self, code_raw, t=None):
		code = code_raw
		if isinstance(code, int): code = f'{code:04d}'
		if code.isdigit(): code = code[:4]
		else: code = self.conf.serw_crs_nlc_map.get(code)
		if code and len(code) == 4: return code
		# XXX: fallback online lookup for arbitrary station name/code via /config/stations
		# async with self.http.get(
		# 		self.api_url('config/stations', search=crs, type=t.value) ) as res:
		# 	await res.json()
		raise GWCError(f'Falied to process station code to 4-digit nlc: {code_raw!r}')

	async def get_journeys(self, src, dst, ts_dep=None):
		'''Query API and return a list of journeys from src to dst,
				starting at ts_dep UTC/BST datetime (without timezone, default: now) or later.
			ts_dep can be either datetime or tuple to specify departure date/time range.
			Default ts_dep range if only one datetime is specified is (ts_dep, ts_dep+3h).'''
		src = await self.get_station(src, self.st_type.src)
		dst = await self.get_station(dst, self.st_type.dst)

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

		jp_res = await self.api_call( 'post', 'jp/journey-plan',
			dict( origin=src, destination=dst,
				outward=dict(rangeStart=ts_start, rangeEnd=ts_end, arriveDepart='Depart'),
				numJourneys=self.conf.test_trip_journeys, adults=1, children=0,
				openReturn=False, disableGroupSavings=True, showCheapest=False, doRealTime=False ) )
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
			src, dst = (trip.stops[n].crs for n in [0, -1])
			jns = await self.get_journeys(src, dst, ts_dep=(trip.ts_start, trip.ts_end))

			## Find one-direct-trip journey with matching train_uid
			for jn in jns:
				if len(jn.trips) > 1: continue
				jn_trip = jn.trips[0]
				if jn_trip.train_uid != trip.train_uid: continue
				if 'nojourney' in fail:
					jn_trip.train_uid += 'x'
					continue
				break
			else: raise GWCTestFailNoJourney(self.api_tag, trip, jns)

			## Match all stops/stop-times
			# SERW API returns non-public stops, which are missing in gtfs
			jn_stops_iter, mismatch_n = iter(jn_trip.stops), random.randrange(0, len(trip.stops))
			for n, st1 in enumerate(trip.stops):
				if n == mismatch_n and 'stopnotfound' in fail: st1.crs += 'x'
				for st2 in jn_stops_iter:
					if st1.crs == st2.crs: break
					if st2.ts:
						raise GWCTestFailStopNotFound(
							self.api_tag, trip, [jn_trip, st2], diff=self.format_trip_diff(trip, jn_trip) )
				else:
					raise GWCTestFailStopNotFound(
						self.api_tag, trip, [jn_trip, st1], diff=self.format_trip_diff(trip, jn_trip) )
				if n == mismatch_n and 'stopmismatch' in fail:
					st1.ts = st2.ts + dt.timedelta(seconds=self.conf.test_trip_time_slack + 5*60)
				ts_diff = abs(st1.ts.total_seconds() - st2.ts.total_seconds())
				if ts_diff > self.conf.test_trip_time_slack:
					raise GWCTestFailStopMismatch( self.api_tag, trip,
						diff=self.format_trip_diff(trip, jn_trip),
						data=[jn_trip, st1.crs, (st1.ts, st2.ts), (ts_diff, self.conf.test_trip_time_slack)] )


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

	async def qb(self, q, *params, c=None, **kws):
		c = self.c if not c else (self.db_cursors.get(c) or await self.connect(c))
		await c.execute(q, params)
		return await c.fetchall()

	def escape(self, val):
		return self.db.escape(val)


	async def _check_trip(self, trip, max_parallel):
		'''Runs trip check on random apis with specified
			max concurrency, preferring ones that are ready first.'''
		api_list, pending = list(self.api_list), list()
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

	async def check_trip(self, trip, ts_start=None):
		'''Checks trip info against API, passing
			raised GWCTestFail exception(s) wrapped into GWCTestBatchFail.'''
		if ts_start: trip = trip.copy(ts_start=ts_start)
		tm, tm_parallel = self.conf.test_match, self.conf.test_match_parallel
		if tm is tm.any: tm_parallel = 1
		test_result_iter = self._check_trip(trip, tm_parallel)
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


	def pick_dates(self, dates):
		'Pick dates to test according to weights in conf.test_pick_date.'
		dates, weights = list(dates), self.conf.test_pick_date or dict(seq=1)
		dates_pick, dates_iter = list(), iter(dates)
		dates_holidays = (self.conf.bank_holidays or set()).intersection(dates)
		while len(dates_pick) < min(len(dates), self.conf.test_trip_dates):
			pick = random_weight(weights)
			if pick == 'seq':
				for date in dates_iter:
					if date not in dates_pick: break
				dates_pick.append(date)
			elif pick == 'bank_holiday':
				if not dates_holidays: continue
				dates_pick.append(dates_holidays.pop())
			elif pick == 'random':
				while True:
					date = random.choice(list(set(dates).difference(dates_pick)))
					if date not in dates_pick: break
				dates_pick.append(date)
			else: raise ValueError(pick)
		return dates_pick

	async def _pick_trips(self):
		test_train_uids = self.conf.test_train_uids
		if isinstance(test_train_uids, int):
			test_train_uids = set(map(op.itemgetter(0), await self.qb(
				'SELECT trip_headsign FROM trips GROUP BY trip_headsign LIMIT %s', test_train_uids )))
		else: test_train_uids = set(test_train_uids)
		if test_train_uids:
			train_uid_join = ','.join(map(self.escape, test_train_uids))
			train_uid_join = f'''
				JOIN
					( SELECT trip_headsign
						FROM trips
						WHERE trip_headsign IN ({train_uid_join})
						GROUP BY trip_headsign ) r
					USING(trip_headsign)'''
		else: train_uid_join = ''

		trip_ids = await self.qb(
			f'SELECT trip_id FROM trips t {train_uid_join}' )
		yield map(op.itemgetter(0), trip_ids)

		q_base = f'''
			SELECT %s AS t, t.*, st.*
			FROM trips t
			{train_uid_join}
			LEFT JOIN stop_times st USING(trip_id)
			{{}}
			ORDER BY t.trip_id, st.stop_sequence'''
		trip_iters = dict(
			(k, self.q(q_base.format(check), k, c=f'trips_{k}'))
			for k, check in dict( seq='',
				assoc=r"WHERE trip_headsign LIKE '%%\_%%'",
				z="WHERE trip_headsign LIKE 'Z%%'" ).items() )
		trip_buffs = dict.fromkeys(trip_iters, (..., None))
		if set(self.conf.test_pick_trip).difference(trip_iters):
			raise ValueError(self.conf.test_pick_trip)

		while trip_iters:
			pick = random_weight(self.conf.test_pick_trip, trip_iters)
			stops_trip_id, stops = trip_buffs[pick]
			while True:
				try:
					t = await anext(trip_iters[pick])
					trip_id, train_uid, stop = t.trip_id, t.trip_headsign, t
				except AsyncStopIteration:
					trip_id = train_uid = stop = None
					del trip_iters[pick]
				else:
					if ( train_uid and test_train_uids is not None
						and train_uid not in test_train_uids ): continue
				if trip_id != stops_trip_id:
					trip_buffs[pick] = trip_id, [stop]
					if stops:
						yield stops
						if test_train_uids is not None:
							test_train_uids.discard(stops[0].trip_headsign)
						break
					elif not trip_id: break
					stops_trip_id, stops = trip_buffs[pick]
				else: stops.append(stop)


	async def pick_trips(self):
		self.stats['trip-skip-set-init'] = len(self.trip_skip)
		trips = self._pick_trips()

		trip_ids = await anext(trips)
		yield len(set(trip_ids).difference(self.trip_skip))

		async for stops in trips:
			trip_id = stops[0].trip_id
			if trip_id in self.trip_skip:
				self.stats['trip-skip-set'] += 1
				continue
			yield stops
			self.trip_skip.add(trip_id)
			if self.trip_log: self.trip_log.write(f'{trip_id}\n')


	async def run(self):
		self.stats = collections.Counter()
		self.stats['diff-total'] = 0

		# XXX: loop until some coverage level
		# XXX: ordering - pick diff types of schedules, train_uids, etc

		trips = self.pick_trips()
		trip_count = await anext(trips)
		progress = progress_iter(self.log, 'trips', trip_count)
		self.stats['trip-count'] = trip_count

		async for stops in trips:
			ts = dt.datetime.now()
			date_current, time_current = ts.date(), ts.time()
			trip_id, train_uid, service_id = op.attrgetter(
				'trip_id', 'trip_headsign', 'service_id' )(stops[0])
			self.log.debug(
				'Checking trip: id={} train_uid={} service_id={}',
				trip_id, train_uid, service_id )

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
			dates = self.pick_dates(dates)
			if not dates:
				self.stats['trip-skip-past'] += 1
				continue

			# Check produced trip info against API(s)
			self.stats['trip-check'] += 1
			for date in dates:
				ts_src = dt.datetime.combine(date, time0) - self.conf.test_trip_embark_delay
				self.stats['trip-check-date'] += 1
				try: await self.check_trip(trip, ts_start=ts_src)
				except GWCTestBatchFail as err_batch:
					self.stats['diff-total'] += 1
					for err in err_batch.exc_list:
						err_type = err.__class__.__name__
						if not isinstance(err, GWCTestFail):
							self.log.error('Failed to check trip: {} - [{}] {}', trip, err_type, err)
							raise err from None
						self.stats[f'diff-api-{err.api}'] += 1
						self.stats[f'diff-type-{err_type}'] += 1
						log_lines(self.log_diffs.error, [
							('API [{}] data mismatch for gtfs trip: {}', err.api, err_type),
							('Trip: {}', trip), ('Date/time: {}', ts_src), 'Diff details:',
							*textwrap.indent(err.diff or pformat_data(err.data), '  ').splitlines() ])

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
		try: await tester.run()
		except asyncio.CancelledError as err: pass
		except GWCTestFoundDiffs: exit_code = 174 # LSB Init Script Actions: 150-199
		else: exit_code = 0
	return exit_code


def main(args=None, conf=None):
	if not conf: conf = TestConfig()

	import argparse
	parser = argparse.ArgumentParser(
		description='Tool to test gtfs feed (stored in mysql) against online data sources.')

	group = parser.add_argument_group('Testing options')
	group.add_argument('-s', '--trip-id-log', metavar='path',
		help='Append each checked trip_id to specified file, and skip ones that are already there.')
	group.add_argument('-f', '--diff-log', metavar='path',
		help='Log diffs to a specified file'
				' (using WatchedFileHandler) instead of stderr that default logging uses.'
			' "-" or "1" can be used for stdout, any integer value for other open fds.')
	group.add_argument('--diff-log-fmt', metavar='format',
			help='Log line format for --diff-log for python stdlib logging module.')
	group.add_argument('-n', '--test-train-limit', type=int, metavar='n',
		help='Randomly pick specified number of distinct train_uids for testing, ignoring all others.')
	group.add_argument('--test-train-uid', metavar='uid-list',
		help='Test entries for specified train_uid only. Multiple values are split by spaces.')

	group = parser.add_argument_group('MySQL db parameters')
	group.add_argument('-d', '--gtfs-db-name',
		default='gtfs', metavar='db-name',
		help='Database name to read GTFS data from (default: %(default)s).')
	group.add_argument('--mycnf-file',
		metavar='path', default=str(pathlib.Path('~/.my.cnf').expanduser()),
		help='Alternative ~/.my.cnf file to use to read all connection parameters from.'
			' Parameters there can include: host, port, user, passwd, connect,_timeout.'
			' Overidden parameters:'
				' db (specified via --src-cif-db/--dst-gtfs-db options),'
				' charset=utf8mb4 (for max compatibility).')
	group.add_argument('--mycnf-group', metavar='group',
		help='Name of "[group]" (ini section) in ~/.my.cnf ini file to use parameters from.')

	group = parser.add_argument_group('Extra data sources')
	group.add_argument('--bank-holiday-list',
		metavar='file', default='doc/UK-bank-holidays.csv',
		help='List of dates, one per line, for bank holidays,'
			' used only for testing priorities. Default: %(default)s')
	group.add_argument('--bank-holiday-fmt',
		metavar='strptime-format', default='%d-%b-%Y',
		help='strptime() format for each line in --bank-holiday-list file. Default: %(default)s')
	group.add_argument('--serw-crs-nlc-csv',
		metavar='file', default='doc/UK-stations-crs-nlc.csv',
		help='UK crs-to-nlc station code mapping table ("crs,nlc" csv file).'
			' Either of these codes can be used for data lookups.'
			' Empty value or "-" will create empty mapping. Default: %(default)s')

	group = parser.add_argument_group('Misc dev/debug options')
	group.add_argument('--debug-cache-dir', metavar='path',
		help='Cache API requests to dir if missing, or re-use cached ones from there.')
	group.add_argument('--debug-http-dir', metavar='path',
		help='Directory path to dump http various responses and headers to.')
	group.add_argument('-x', '--debug-trigger-mismatch', metavar='type',
		help='Trigger data mismatch of specified type in all tested entries.'
			' Supported types correspond to implemented GWCTestFail'
				' exceptions, e.g.: NoJourney, StopNotFound, StopMismatch.'
			' Multiple values can be specified in one space-separated arg.')
	group.add_argument('--debug', action='store_true', help='Verbose operation mode.')

	opts = parser.parse_args(sys.argv[1:] if args is None else args)

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

	if opts.bank_holiday_list:
		conf.bank_holidays = set()
		with pathlib.Path(opts.bank_holiday_list).open() as src:
			for line in src.read().splitlines():
				conf.bank_holidays.add(dt.datetime.strptime(line, opts.bank_holiday_fmt).date())

	conf.mysql_conn_opts = dict(filter(op.itemgetter(1), dict(
		read_default_file=opts.mycnf_file, read_default_group=opts.mycnf_group ).items()))
	conf.mysql_db_name = opts.gtfs_db_name

	if opts.test_train_uid:
		conf.test_train_uids = opts.test_train_uid.split()
		if opts.test_train_limit:
			conf.test_train_uids = conf.test_train_uids[:opts.test_train_limit]
	else: conf.test_train_uids = opts.test_train_limit

	if opts.debug_http_dir: conf.debug_http_dir = pathlib.Path(opts.debug_http_dir)
	if opts.debug_cache_dir: conf.debug_cache_dir = pathlib.Path(opts.debug_cache_dir)
	if opts.debug_trigger_mismatch: conf.debug_trigger_mismatch = opts.debug_trigger_mismatch

	log.debug('Starting run_tests loop...')
	with contextlib.closing(asyncio.get_event_loop()) as loop:
		exit_code = loop.run_until_complete(run_tests(loop, conf))
	log.debug('Finished')
	return exit_code

if __name__ == '__main__': sys.exit(main())
