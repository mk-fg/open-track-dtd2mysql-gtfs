#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
import os, sys, pathlib, logging, signal, locale, warnings
import contextlib, inspect, collections, enum, time, datetime
import asyncio, urllib.parse, json, re

import aiohttp # http://aiohttp.readthedocs.io
import aiomysql # http://aiomysql.readthedocs.io


class TestConfig:

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

	debug_http_dir = None


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


# XXX: separate data sources and test types

class GWCError(Exception): pass
class GWCAPIError(GWCError): pass
class GWCAPIErrorCode(GWCAPIError): pass

class GWC:

	def __init__(self, loop, conf, log):
		self.loop, self.conf = loop, conf
		self.log = log or get_logget('gwc.test')
		self.debug_files = collections.Counter()

	async def __aenter__(self):
		self.ctx = AsyncExitStack()

		# Reset locale for consistency in calendar and such
		locale_prev = locale.setlocale(locale.LC_ALL, '')
		self.ctx.callback(locale.setlocale, locale.LC_ALL, locale_prev)

		# Warnings from aiomysql about buffered results and such are all bugs
		await self.ctx.enter(warnings.catch_warnings())
		warnings.filterwarnings('error')

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
		self.log.debug('serw api call: {} {}', method, p)
		try:
			async with self.http.request( method,
					self.api_url(p, **(q or dict())), json=j, headers=headers ) as res:
				if res.content_type != 'application/json':
					data = await res.read()
					raise GWCAPIError(res.status, f'non-json response - {data!r}')
				data = await res.json()
				if self.conf.debug_http_dir:
					fn = 'api.{}.res.{{}}.json'.format(p.replace('/', '-').replace('.', '_'))
					self.debug_files[fn] += 1
					with ( self.conf.debug_http_dir /
						fn.format(self.debug_files[fn]) ).open('w') as dst: json.dump(data, dst)
				if isinstance(data, dict) and data.get('errors'):
					err = data['errors']
					try:
						err, = err
						err = err['errorCode'], err.get('failureType')
					except (TypeError, ValueError, IndexError):
						raise GWCAPIError(res.status, err)
					else: raise GWCAPIErrorCode(res.status, *err)
				if res.status != 200: raise GWCAPIError(res.status, 'non-200 response status')
		except aiohttp.ClientError as err:
			raise GWCAPIError(None, f'[{err.__class__.__name__}] {err}') from None
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

	async def get_calling_points(self, src, dst, ts_start=None, ts_end=None):
		src = await self.get_station(src, self.st_type.src)
		dst = await self.get_station(dst, self.st_type.dst)

		# Default is to use current time and +2d as ts_end
		if not ts_start:
			ts = datetime.datetime.now()
			ts -= datetime.timedelta(seconds=time.localtime().tm_gmtoff) # to utc
			if ( (ts.month > 3 or ts.month < 10) # "mostly correct" (tm) DST hack
					or (ts.month == 3 and ts.day >= 27)
					or (ts.month == 10 and ts_start.day <= 27) ):
				ts += datetime.timedelta(seconds=3600)
			ts_start = ts
		if not ts_end:
			ts_end = ts_start + datetime.timedelta(days=2)
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

		for jp_url in jp_urls:
			journey = await self.api_call('get', f'{jp_url}/calling-points')


	async def run(self):
		trip_info = await self.get_calling_points('SHF', 'LBG')
		print(trip_info)


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
	async with GWC(loop, conf, log) as tester:
		# XXX: iterate over different test types and counts, run them in some order
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
	# XXX: specify which tests to run and when to stop

	group = parser.add_argument_group('Extra data sources')
	group.add_argument('--serw-crs-nlc-csv',
		metavar='file', default='doc/UK-stations-crs-nlc.csv',
		help='UK crs-to-nlc station code mapping table ("crs,nlc" csv file).'
			' Either of these codes can be used for data lookups.'
			' Empty value or "-" will create empty mapping. Default: %(default)s')

	group = parser.add_argument_group('Misc/debug options')
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

	if opts.debug_http_dir:
		conf.debug_http_dir = pathlib.Path(opts.debug_http_dir)

	log.debug('Starting run_tests loop...')
	with contextlib.closing(asyncio.get_event_loop()) as loop:
		exit_code = loop.run_until_complete(run_tests(loop, conf))
	log.debug('Finished')
	return exit_code

if __name__ == '__main__': sys.exit(main())
