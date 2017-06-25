#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
import os, sys, contextlib, logging, pathlib
import collections, enum
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


class ConversionError(Exception): pass
class CIFError(ConversionError): pass


GTFSRouteType = enum.IntEnum( 'RouteType',
	'light_rail subway rail bus ferry cable_car gondola funicular spacecraft' )
GTFSPickupType = enum.IntEnum('PickupType', 'regular none phone driver')


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

	def q(self, q, *params, fetch=True):
		with self.db.cursor(NTCursor) as cur:
			if self.log_sql.isEnabledFor(logging.DEBUG):
				p_log = str(params)
				if len(p_log) > 150: p_log = f'{p_log[:150]}...[len={len(p_log)}]'
				self.log_sql.debug('{!r} {}', ' '.join(q.split()), p_log)
			cur.execute(q, params)
			return cur.fetchall() if fetch else cur.lastrowid

	def insert(self, table, **row):
		row = collections.OrderedDict(row.items())
		cols, vals = ','.join(row.keys()), ','.join(['%s']*len(row))
		return self.q( f'INSERT INTO'
			f' {table} ({cols}) VALUES ({vals})', *row.values(), fetch=False )

	def run(self):
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


		### DTD Schedule/Service entries
		# XXX: add processing of Z schedules here as well
		# XXX: associations

		route_ids = dict() # {(src, dst}: id}
		for s in q('''
				SELECT *
				FROM cif.schedule s
				LEFT JOIN cif.schedule_extra e ON e.schedule = s.id
				ORDER BY s.id
				LIMIT 100'''): # limit for testing only

			# XXX: handle overrides later
			if s.stp_indicator != 'P': continue


			### Get route information and create Route, ignoring stop times for now

			stops = list(q('''
				SELECT * FROM cif.stop_time st
				JOIN cif.tiploc t ON t.tiploc_code = st.location
				JOIN gtfs.stops s ON t.crs_code = s.stop_id
				WHERE schedule = %s
				ORDER BY
					public_arrival_time, scheduled_arrival_time,
					public_departure_time, scheduled_departure_time''', s.id))
			if not stops:
				log.info('Skipping schedule with no usable stops: {}', s)
				continue

			# XXX: note on route_ids? - "how to do routes? get the toc in from schedule_extra"
			route_key = stops[0].id, stops[-1].id
			if route_key in route_ids: route_id = route_ids[route_key]
			else:
				route_id = route_ids[route_key] = insert( 'gtfs.routes',
					route_short_name=f'{stops[0].crs_code}-{stops[-1].crs_code}',
					route_long_name='From {} to {}'.format(*(
						(stops[n].description.title() or stops[n].crs_code) for n in [0, -1] )),
					route_type=int(GTFSRouteType.rail) )


			### Trip
			# XXX: fill in more metadata here
			trip_id = insert( 'gtfs.trips',
				route_id=route_id, service_id=s.id, trip_id=s.id,
				trip_headsign=s.train_uid, trip_short_name=s.retail_train_id )

			### Trip stop times
			# XXX: time conversions - DTD->GTFS and late->next-day
			for n, st in enumerate(stops, 1):

				public_stop = st.public_arrival_time or st.public_departure_time
				if public_stop:
					pickup_type = GTFSPickupType.regular
					ts_arr, ts_dep = st.public_arrival_time, st.public_departure_time
					# XXX: check if origin has some arrival time indication in CIF data
					ts_arr, ts_dep = ts_arr or ts_dep, ts_dep or ts_arr # origin/termination stops
				else:
					pickup_type = GTFSPickupType.none
					ts_arr, ts_dep = st.scheduled_arrival_time, st.scheduled_departure_time

				insert( 'gtfs.stop_times',
					trip_id=trip_id,
					arrival_time=ts_arr, departure_time=ts_dep,
					stop_id=st.stop_id, stop_sequence=n,
					pickup_type=int(pickup_type) )


			### Service Calendar dates
			insert( 'gtfs.calendar',
				service_id=s.id, start_date=s.runs_from, end_date=s.runs_to,
				**dict((k.lower(), getattr(s, k.lower())) for k in calendar.day_name) )

			# XXX: add gtfs.calendar date exceptions/overrides from cif.calendar
			#
			# find any other trip with the same train_uid
			#  where this services dates are inside the other services dates
			# SELECT * FROM calendar JOIN trips USING(service_id)
			# WHERE
			#  trip_headsign = schedule.train_uid
			#  AND runs_from < schedule.runs_from AND runs_to > schedule.runs_to
			# chop the runs_to of the original schedule to be the runs_from of this schedule
			# insert another calendar date for the service that
			#  runs_from the runs_to date of this schedule and runs_to the original runs_to
			#
			# note: rather than doing several chops
			#  it might be worth detecting small overlays and adding them as exceptions
			# for an example, run:
			#  select id, train_uid, runs_from, runs_to,
			#  stp_indicator, train_identity, train_category,
			#  headcode from schedule where train_uid = "Y03228" order by id;

		# XXX: populate gtfs.calendar_dates from bank holiday list
		# # note: I think we only need to add the excludes
		# FOREACH BANK HOLIDAY
		#  INSERT INTO calendar_dates
		#  SELECT id, :date, 2 FROM schedule
		#  WHERE bank_holiday_running = 0 AND :date BETWEEN runs_from AND runs_to


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

	# Useful stuff for ~/.my.cnf: host port user passwd connect_timeout
	mysql_conn_opts = dict(filter(op.itemgetter(1), dict(
		read_default_file=opts.mycnf_file, read_default_group=opts.mycnf_group ).items()))
	with DTDtoGTFS(opts.src_cif_db, opts.dst_gtfs_db, mysql_conn_opts) as conv: conv.run()

if __name__ == '__main__': sys.exit(main())
