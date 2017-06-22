#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
import os, sys, contextlib, logging, pathlib, collections

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


class DBCursor(MySQLdb.cursors.Cursor):
	'Returns named tuples for specified list of fields without any introspection.'

	@classmethod
	@ft.lru_cache(maxsize=128)
	def for_fields(cls, fields):
		return ft.partial(cls,
			(lambda *a: tuple(a)) if isinstance(fields, str) else
			collections.namedtuple(f'Row', fields, rename=True) )

	def __init__(self, tuple_type, *args, **kws):
		self._tt = tuple_type
		super().__init__(*args, **kws)

	def fetchone(self):
		for row in super().fetchone(): yield self._tt(*row)
	def fetchmany(self, size=None):
		for row in super().fetchmany(size=size): yield self._tt(*row)
	def fetchall(self):
		for row in super().fetchall(): yield self._tt(*row)


class DTDtoGTFS:

	# Forces errors on truncated values and issues in multi-row inserts
	sql_mode = 'strict_all_tables'

	def __init__(self, db_cif, db_gtfs, conn_opts_base):
		self.db_cif, self.db_gtfs, self.conn_opts_base = db_cif, db_gtfs, conn_opts_base
		self.log = get_logger('dtd2gtfs')

	def __enter__(self):
		self.db = MySQLdb.connect(**self.conn_opts_base)
		with self.db.cursor() as cur:
			cur.execute('show variables like %s', ['sql_mode'])
			mode_flags = set(map(str.strip, dict(cur.fetchall())['sql_mode'].lower().split(',')))
			mode_flags.update(self.sql_mode.lower().split())
			cur.execute('set sql_mode = %s', [','.join(mode_flags)])
		return self

	def __exit__(self, *exc):
		if self.db: self.db = self.db.close()


	## Simple query builder

	def q_raw(self, q, *params, ct=None):
		with self.db.cursor(ct) as cur:
			self.log.debug('sql-raw: {!r} {}', q, params or None)
			cur.execute(q, params)
			return list(cur.fetchall())

	def q( self,
			table, fields='*', raw_filter=None, ext=None,
			col=None, v=None, chk='=', to=None, cursor=False ):
		params, cursor_type = list(), None
		if not isinstance(fields, str):
			fields, cursor_type = ', '.join(fields), \
				DBCursor.for_fields(tuple(f.split()[-1] for f in fields))
		if not raw_filter and col:
			raw_filter = f'where {col} {chk} %s'
			params.append(v)
		raw_filter, ext = raw_filter or '', ext or ''
		with self.db.cursor(cursor_type) as cur:
			q = f'select {fields} from {self.db_cif}.{table} {raw_filter} {ext}'
			if to: q = f'insert into {self.db_gtfs}.{to} {q}'
			self.log.debug('sql: {!r} {}', q, params)
			cur.execute(q, params)
			if cursor: yield cur
			elif to: return
			else:
				for row in cur.fetchall(): yield row

	@contextlib.contextmanager
	def qc(self, *args, **kws):
		query = self.q(*args, cursor=True, **kws)
		yield next(query)
		next(query)

	def qe(self, *args, **kws):
		query = self.q(*args, cursor=True, **kws)
		return next(query)


	## Conversion routine

	def run(self):
		self.qe('fixed_link', 'null, origin, destination, 2, duration * 60', to='transfers')
		with self.qc('schedule', ['id', 'train_uid']) as c:
			print(c.rowcount)
		for row in self.q('schedule', ['id', 'train_uid'], ext='limit 1'):
			print(row)
			break


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
	mysql_conn_opts = dict(filter(op.itemgetter(1), dict( charset='utf8mb4',
		read_default_file=opts.mycnf_file, read_default_group=opts.mycnf_group ).items()))
	with DTDtoGTFS(opts.src_cif_db, opts.dst_gtfs_db, mysql_conn_opts) as conv: conv.run()

if __name__ == '__main__': sys.exit(main())
