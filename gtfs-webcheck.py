#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
import datetime as dt
import os, sys, pathlib, logging, signal, locale, warnings
import contextlib, inspect, collections, enum, time
import asyncio, urllib.parse, json, re, random

import aiohttp # http://aiohttp.readthedocs.io
import aiomysql # http://aiomysql.readthedocs.io


class TestConfig:

	# test_match: how to select API to match gtfs data (e.g. trip) against.
	#  all - query all APIs for match
	#  any - query one API at random
	#  first - query APIs until match is found
	test_match = enum.Enum('TestMatch', 'all any first').any
	test_match_parallel = 1 # number of APIs to query in parallel, only for test_match=all/first

	# test_pick: order in which gtfs data is selected for testing.
	# test_pick = enum.Enum('tp', 'random random_consistent column?')

	# test_train_uids: either integer to pick n random train_uids or list of specific uids to use.
	test_train_uids = None

	mysql_db_name = None
	mysql_conn_opts = dict()
	mysql_sql_mode = 'strict_all_tables'

	serw_api_url = 'https://api.southeasternrailway.co.uk'
	serw_crs_nlc_map = None
	serw_http_headers = {
		'Accept': 'application/json',
		'Content-Type': 'application/json',
		'Origin': 'https://ticket.southeasternrailway.co.uk',
		'x-access-token':
			'otrl|a6af56be1691ac2929898c9f68c4b49a0a2d930849770dba976be5d792a',
		# 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:54.0) Gecko/20100101 Firefox/54.0',
	}

	rate_interval = 3 # seconds
	rate_max_concurrency = 1
	rate_min_seq_delay = 0 # seconds, only if rate_max_concurrency=1

	debug_http_dir = None
	debug_cache_dir = None

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

async def anext(aiter): # py3.7
	async for v in aiter: return v
	raise StopIteration


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

popn = lambda v,n: list(v.pop() for m in range(n))
url_to_fn = lambda p: p.replace('/', '-').replace('.', '_')


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


class GWCError(Exception): pass
class GWCAPIError(GWCError): pass
class GWCAPIErrorCode(GWCAPIError): pass


class GWCTripStop:

	def __init__(self, crs, nlc, ts, pickup, dropoff, **meta):
		self.crs, self.nlc, self.ts, self.pickup, self.dropoff = crs, nlc, ts, pickup, dropoff
		self.meta = meta

	def __repr__(self):
		embark = '-P'[bool(self.pickup)] + '-D'[bool(self.dropoff)]
		ts = self.ts.strftime('%H:%M') if self.ts else 'x'
		return f'<TS {self.crs} {self.nlc} {embark} {ts}>'

class GWCTrip:

	TripSig = collections.namedtuple('TripSig', 'src dst train_uid ts_src')

	@classmethod
	def from_serw_cps(cls, sig, stops, links):
		embark_flags = dict(
			Normal=(True, True), Passing=(False, False),
			PickUpOnly=(True, False), SetDownOnly=(False, True) )
		src, dst, trip_stops = sig.src, sig.dst, list()
		for stop in stops:
			stop_info = links[stop['station']]
			pickup, dropoff = embark_flags[stop['pattern']]
			ts = ( None if not (pickup or dropoff) else
				dt.datetime.strptime(stop['time']['scheduledTime'], '%Y-%m-%dT%H:%M:%S') )
			name, crs, nlc, lat, lon = op.itemgetter(
				'name', 'crs', 'nlc', 'latitude', 'longitude' )(stop_info)
			if src and src != crs: continue
			src = trip_stops.append(GWCTripStop(
				crs, nlc, ts, pickup, dropoff, name=name, lat=lat, lon=lon ))
			if dst == crs: break
		return cls(sig.train_uid, sig.ts_src, trip_stops)

	def __init__(self, train_uid, ts_start, stops):
		self.train_uid, self.ts_start, self.stops = train_uid, ts_start, stops

	def __repr__(self):
		return (
			f'<Trip {self.train_uid} [{self.ts_start}]'
				f' [{" - ".join(ts.crs for ts in self.stops)}]>' )


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
			if cache_fn.exists():
				self.log.debug('serw-api cache-read: {}', cache_fn)
				return json.loads(cache_fn.read_text())
		try:
			async with self.http.request( method,
					self.api_url(p, **(q or dict())), json=j, headers=headers ) as res:
				if res.content_type != 'application/json':
					data = await res.read()
					raise GWCAPIError(res.status, f'non-json response - {data!r}')
				data = await res.json()
				if self.conf.debug_http_dir:
					fn = f'api-req.{url_to_fn(p)}.res.{{:03d}}.json'
					self.conf._debug_files[fn] += 1
					with ( self.conf.debug_http_dir /
							fn.format(self.conf._debug_files[fn]) ).open('w') as dst:
						json.dump(data, dst)
				if isinstance(data, dict) and data.get('errors'):
					err = data['errors']
					try:
						err, = err
						err = err['errorCode'], err.get('failureType')
					except (TypeError, ValueError, IndexError):
						raise GWCAPIError(res.status, err)
					else: raise GWCAPIErrorCode(res.status, *err)
				elif isinstance(data, dict) and 'result' not in data:
					raise GWCAPIError(res.status, f'no "result" key in data - {data!r}')
				if res.status != 200: raise GWCAPIError(res.status, 'non-200 response status')
		except aiohttp.ClientError as err:
			raise GWCAPIError(None, f'[{err.__class__.__name__}] {err}') from None
		if self.conf.debug_cache_dir:
			self.log.debug('serw-api cache-write: {}', cache_fn)
			cache_fn.write_text(json.dumps(data))
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

	async def get_journeys(self, src, dst, ts_start=None, ts_end=None):
		src = await self.get_station(src, self.st_type.src)
		dst = await self.get_station(dst, self.st_type.dst)

		# Default is to use current time and +2d as ts_end
		if not ts_start:
			ts = dt.datetime.now()
			ts -= dt.timedelta(seconds=time.localtime().tm_gmtoff) # to utc
			if ( (ts.month > 3 or ts.month < 10) # "mostly correct" (tm) DST hack
					or (ts.month == 3 and ts.day >= 27)
					or (ts.month == 10 and ts_start.day <= 27) ):
				ts += dt.timedelta(seconds=3600)
			ts_start = ts
		if not ts_end:
			ts_end = ts_start + dt.timedelta(days=2)
		ts_start, ts_end = (
			(ts if isinstance(ts, str) else ts.strftime('%Y-%m-%dT%H:%M:%S'))
			for ts in [ts_start, ts_end] )

		jp_res = await self.api_call( 'post', 'jp/journey-plan',
			dict( origin=src, destination=dst,
				outward=dict(rangeStart=ts_start, rangeEnd=ts_end, arriveDepart='Depart'),
				numJourneys=3, adults=1, children=0,
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
		async with self.rate_sem:
			# XXX: fetch corresponding info from api and test against trip
			src, dst = trip
			jns = await self.get_journeys(src, dst)
			print('trip:', trip)
			print('journeys:', jns)
			return True


class GWCTestRunner:
	'Fetch trips from mysql at random and test them against specified APIs.'

	def __init__(self, loop, conf, api_list):
		self.loop, self.conf = loop, conf
		self.log = get_logger('gwc.test')
		self.api_list = list(api_list)

	async def __aenter__(self):
		self.ctx = AsyncExitStack()

		# Reset locale for consistency in calendar and such
		locale_prev = locale.setlocale(locale.LC_ALL, '')
		self.ctx.callback(locale.setlocale, locale.LC_ALL, locale_prev)

		# Warnings from aiomysql about buffered results and such are all bugs
		await self.ctx.enter(warnings.catch_warnings())
		warnings.filterwarnings('error')

		self.db_conns, self.db_cursors = dict(), dict()
		self.db_pool = await self.ctx.enter(
			aiomysql.create_pool(charset='utf8mb4', **self.conf.mysql_conn_opts) )
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


	async def run_test(self, trip, max_parallel):
		'''Runs test on random apis with specified
			max concurrency, preferring ones that are ready first.'''
		api_list, pending = list(self.api_list), list()
		random.shuffle(api_list)
		while api_list or pending:
			if api_list and len(pending) < max_parallel:
				api_list.sort(key=op.methodcaller('ready'))
				while len(pending) < max_parallel:
					pending.append(self.loop.create_task(api_list.pop().test_trip(trip)))
			done, pending = await asyncio.wait(
				pending, return_when=asyncio.FIRST_COMPLETED )
			for fut in done: yield fut.result()


	async def get_trips(self):
		train_uid_slice = self.conf.test_train_uids
		if isinstance(train_uid_slice, int): # XXX: randomize
			train_uid_slice = f'''
				JOIN
					( SELECT trip_headsign AS train_uid
						FROM trips
						GROUP BY trip_headsign
						LIMIT {train_uid_slice} ) r
					ON r.train_uid = t.trip_headsign'''
		elif train_uid_slice:
			train_uid_slice = ','.join(map(self.escape, train_uid_slice))
			train_uid_slice = f'''
				JOIN
					( SELECT trip_headsign AS train_uid
						FROM trips
						WHERE train_uid IN ({train_uid_slice})
						GROUP BY trip_headsign ) r
					ON r.train_uid = t.trip_headsign'''
		else: train_uid_slice = ''
		trip_count = await self.qb(
			f'SELECT COUNT(*) FROM trips t {train_uid_slice}' )
		yield trip_count[0][0]
		trips = self.q(f'''
			SELECT
				t.trip_id,
				t.service_id AS svc_id,
				st.stop_id AS id,
				st.stop_sequence AS seq,
				st.arrival_time AS ts_arr,
				st.departure_time AS ts_dep
			FROM trips t
			{train_uid_slice}
			LEFT JOIN stop_times st USING(trip_id)
			ORDER BY t.trip_id, st.stop_sequence''')
		trip_id, stops = ..., None
		async for t in trips:
			if t.trip_id != trip_id:
				if stops: yield stops
				trip_id, stops = t.trip_id, [t]
			else: stops.append(t)
		yield stops



	async def run(self):
		tm, tm_parallel = self.conf.test_match, self.conf.test_match_parallel
		if tm is tm.any: tm_parallel = 1

		while True: # XXX: loop until some coverage level
			# XXX: ordering - pick diff types of schedules, train_uids, etc

			trips = self.get_trips()
			trip_count = await anext(trips)
			progress = progress_iter(self.log, 'trips', trip_count)

			async for stops in trips:
				t = stops[0]
				for stop in stops: print(stop)
			exit() # XXX

			trip = 'SHF', 'LBG' # XXX: get from gtfs db

			test_result_iter = self.run_test(trip, tm_parallel)

			if tm is tm.all:
				async for res in test_result_iter:
					if not res:
						print('FAIL') # XXX
						break

			elif tm is tm.first:
				async for res in test_result_iter:
					if res: break
				else: print('FAIL') # XXX

			elif tm is tm.any:
				async for res in test_result_iter:
					if not res: print('FAIL') # XXX
					break

			else: raise ValueError(tm)

			print('SUCCESS')


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
		# except GWCError as err:
		else: exit_code = 0
	return exit_code


def main(args=None, conf=None):
	if not conf: conf = TestConfig()

	import argparse
	parser = argparse.ArgumentParser(
		description='Tool to test gtfs feed (stored in mysql) against online data sources.')

	group = parser.add_argument_group('Testing options')
	group.add_argument('-n', '--test-train-limit', type=int, metavar='n',
		help='Randomly pick specified number of distinct train_uids for testing, ignoring all others.')
	group.add_argument('--test-train-uid', metavar='uid-list',
		help='Test entries for specified train_uid only. Multiple values are split by spaces.')

	group = parser.add_argument_group('MySQL db parameters')
	group.add_argument('-d', '--gtfs-db-name',
		default='gtfs', metavar='db-name',
		help='Database name to read GTFS data from (default: %(default)s).')
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
	group.add_argument('--debug', action='store_true', help='Verbose operation mode.')

	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	logging.basicConfig(
		datefmt='%Y-%m-%d %H:%M:%S',
		format='%(asctime)s :: {}%(levelname)s :: %(message)s'\
			.format('%(name)s ' if opts.debug else ''),
		level=logging.DEBUG if opts.debug else logging.INFO )
	log = get_logger('gwc.main')

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

	log.debug('Starting run_tests loop...')
	with contextlib.closing(asyncio.get_event_loop()) as loop:
		exit_code = loop.run_until_complete(run_tests(loop, conf))
	log.debug('Finished')
	return exit_code

if __name__ == '__main__': sys.exit(main())
