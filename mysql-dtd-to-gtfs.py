#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
import os, sys, contextlib, logging, pathlib, collections
import calendar, locale

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

get_logger = lambda name: LogStyleAdapter(logging.getLogger(name))


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


class DTDtoGTFS:

	# Forces errors on truncated values and issues in multi-row inserts
	sql_mode = 'strict_all_tables'

	def __init__(self, db_cif, db_gtfs, conn_opts_base):
		self.db_cif, self.db_gtfs, self.conn_opts_base = db_cif, db_gtfs, conn_opts_base
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

	def q(self, q, *params):
		with self.db.cursor(NTCursor) as cur:
			if self.log_sql.isEnabledFor(logging.DEBUG):
				p_log = str(params)
				if len(p_log) > 150: p_log = f'{p_log[:150]}...[len={len(p_log)}]'
				self.log_sql.debug('{!r} {}', ' '.join(q.split()), p_log)
			cur.execute(q, params)
			return cur.fetchall()

	def insert(self, table, **row):
		row = collections.OrderedDict(row.items())
		cols, vals = ','.join(row.keys()), ','.join(['%s']*len(row))
		self.q(f'INSERT INTO {table} ({cols}) VALUES ({vals})', *row.values())

	def run(self):
		q, insert = self.q, self.insert

		### Stops
		q('''
			INSERT INTO gtfs.stops
					SELECT
						null, crs_code, tiploc_code, description, description,
						null, null, null, null, 0, null, "Europe/London", 0
					FROM cif.tiploc WHERE crs_code IS NOT NULL AND description IS NOT NULL;''')

		### Transfers
		# TODO: better data source / convert to trip+frequences? Losing the mode here, TUBE, WALK, etc
		q('''
			INSERT INTO gtfs.transfers
				SELECT null, origin, destination, 2, duration * 60
				FROM cif.fixed_link;''')
		# This is interchange time
		q('''
			INSERT INTO gtfs.transfers
				SELECT null, crs_code, crs_code, 2, minimum_change_time * 60
				FROM cif.physical_station;''')

		for s in q('''
				SELECT *
				FROM cif.schedule
				LEFT JOIN cif.schedule_extra e ON e.schedule = schedule.id'''):

			### Calendar dates
			insert( 'gtfs.calendar',
				service_id=s.id, start_date=s.runs_from, end_date=s.runs_to,
				**dict((k.lower(), getattr(s, k.lower())) for k in calendar.day_name) )

			### Trips
			# XXX: fill in more metadata here
			insert( 'gtfs.trips',
				route_id='XXX', service_id=s.id, trip_id=s.id,
				trip_headsign=s.train_uid, trip_short_name=s.retail_train_id )


def main(args=None):
	import argparse
	parser = argparse.ArgumentParser(
		description='Tool to convert imported DTD/CIF data'
			' stored in one MySQL db to GTFS feed in another db.')

	group = parser.add_argument_group('MySQL db parameters.')
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

	group = parser.add_argument_group('Misc other options')
	group.add_argument('--debug', action='store_true', help='Verbose operation mode.')

	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	global log
	logging.basicConfig( datefmt='%Y-%m-%d %H:%M:%S',
		format='%(asctime)s :: %(name)s %(levelname)s :: %(message)s',
		level=logging.DEBUG if opts.debug else logging.WARNING )
	log = get_logger('main')

	# Useful stuff for ~/.my.cnf: host port user passwd connect_timeout
	mysql_conn_opts = dict(filter(op.itemgetter(1), dict(
		read_default_file=opts.mycnf_file, read_default_group=opts.mycnf_group ).items()))
	with DTDtoGTFS(opts.src_cif_db, opts.dst_gtfs_db, mysql_conn_opts) as conv: conv.run()

if __name__ == '__main__': sys.exit(main())
