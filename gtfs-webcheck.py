#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
import os, sys, logging, contextlib, signal
import asyncio

import aiohttp # http://aiohttp.readthedocs.io
import aiomysql # http://aiomysql.readthedocs.io


class TestConfig: pass


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

class GWC:

	def __init__(self, loop, conf, log):
		self.loop, self.conf = loop, conf
		self.log = log or get_logget('gwc.test')

	async def __aenter__(self): return self
	async def __aexit__(self, *err): pass

	async def run(self):
		# XXX: initial playground
		pass


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

	group = parser.add_argument_group('Misc/debug options')
	group.add_argument('--debug', action='store_true', help='Verbose operation mode.')

	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	logging.basicConfig(
		datefmt='%Y-%m-%d %H:%M:%S',
		format='%(asctime)s :: {}%(levelname)s :: %(message)s'\
			.format('%(name)s ' if opts.debug else ''),
		level=logging.DEBUG if opts.debug else logging.INFO )
	log = get_logger('gwc.main')

	log.debug('Starting run_tests loop...')
	with contextlib.closing(asyncio.get_event_loop()) as loop:
		exit_code = loop.run_until_complete(run_tests(loop, conf))
	log.debug('Finished')
	return exit_code

if __name__ == '__main__': sys.exit(main())
