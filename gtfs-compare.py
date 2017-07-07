#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
import os, sys, contextlib, logging, pathlib, re, warnings, locale
import collections, time, csv, datetime, pprint, random

import pymysql, pymysql.cursors # https://pymysql.readthedocs.io/
import deepdiff # http://deepdiff.readthedocs.io/


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

class adict(dict):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.__dict__ = self
	def __setattr__(self, k, v):
		if k.startswith('_'): super().__setattr__(k, v)
		self[k] = v

def dts_parse(dts_str):
	if ':' not in dts_str: return float(dts_str)
	dts_vals = dts_str.split(':')
	if len(dts_vals) == 2: dts_vals.append('00')
	assert len(dts_vals) == 3, dts_vals
	return sum(int(n)*k for k, n in zip([3600, 60, 1], dts_vals))

def dts_format(dts):
	if isinstance(dts, str): return dts
	if isinstance(dts, datetime.timedelta): dts = dts.total_seconds()
	dts_days, dts = divmod(int(dts), 24 * 3600)
	dts = str(datetime.time(dts // 3600, (dts % 3600) // 60, dts % 60, dts % 1))
	if dts_days: dts = '{}+{}'.format(dts_days, dts)
	return dts


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

	def compare(self, db1, db2, trip_limit=None):
		log, dbs = self.log, (db1, db2)

		diff_func = deepdiff.DeepDiff
		diff_print = ft.partial(pprint.pprint, indent=2)

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
		tuid_check = tuid_check[:trip_limit or 2**30]
		log.debug('Comparing trips/stops for {} train_uids...', len(tuid_check))

		for train_uid in tuid_check:
			log.debug('Comparing data for train_uid={}...', train_uid)
			stats = dict((db, collections.Counter()) for db in dbs)
			trip_info = dict((db, adict()) for db in dbs)

			# Populate trip_info
			for db in dbs:
				trip_span_idx, trip_stop_idx = collections.defaultdict(set), dict()
				trip_info[db].spans, trip_info[db].stops = trip_span_idx, trip_stop_idx
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
					trip = next(stops)
					stops, trip_stops = [trip, *stops], list()
					for st in stops:
						if not (st.ts_arr or st.ts_dep):
							stats[db]['stop-no-times'] += 1
							continue
						ts_arr = dts_format(st.ts_arr or st.ts_dep)
						ts_dep = dts_format(st.ts_dep or st.ts_arr)
						trip_stops.append((st.id, ts_arr, ts_dep))
					trip_stops, trip_hash = tuple(trip_stops), hash(tuple(trip_stops))
					# if trip_hash in trip_stop_idx: stats[db]['trip-dup-stops'] += 1
					# if trip.svc_id in trip_span_idx[trip_hash]: stats[db]['trip-dup-svc'] += 1
					trip_span_idx[trip_hash].add(trip.svc_id)
					trip_stop_idx[trip_hash] = trip_stops

			log.debug('Comparing trip-stops for train_uid={}...', train_uid)
			diff = diff_func(*(set(trip_info[db].stops.values()) for db in dbs))
			if diff:
				log.info('Stop sequence(s) mismatch for train_uid: {}', train_uid)
				diff_print(diff)

			for db in dbs:
				for k, v in stats[db].items(): log.info('[{}] Quirk count: {}={}', db, k, v)


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
	cmd.add_argument('db1', help='Database-1 to compare Database-2 against.')
	cmd.add_argument('db2', help='Database-2 to compare Database-1 against.')
	cmd.add_argument('-n', '--trip-limit', metavar='n', type=int,
		help='Stop after comparing specified number of trips.')

	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	logging.basicConfig( stream=sys.stdout,
		level=logging.DEBUG if opts.debug else logging.INFO,
		format='%(asctime)s :: %(name)s %(levelname)s :: %(message)s' )
	log = get_logger('main')

	mysql_conn_opts = dict(filter(op.itemgetter(1), dict(
		read_default_file=opts.mycnf_file, read_default_group=opts.mycnf_group ).items()))
	with GTFSDB(mysql_conn_opts) as db:

		if opts.call == 'import':
			db.parse(opts.db_name, opts.src_path, opts.gtfs_schema)

		elif opts.call == 'compare':
			db.compare(opts.db1, opts.db2, opts.trip_limit)

		else: parser.error(f'Action not implemented: {opts.call}')

if __name__ == '__main__': sys.exit(main())
