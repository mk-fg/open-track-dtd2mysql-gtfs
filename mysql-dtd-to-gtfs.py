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
	'light_rail subway rail bus ferry cable_car gondola funicular spacecraft', start=0 )
GTFSPickupType = enum.IntEnum('PickupType', 'regular none phone driver', start=0)
GTFSExceptionType = enum.IntEnum('ExceptionType', 'added removed')


class GTFSTimespanError(Exception): pass
class GTFSTimespan: pass # XXX: implement


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

	def run(self, test_run_slice=None):
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


		### Process cif.schedule entries to gtfs.trips/gtfs.calendar/gtfs.calendar_dates
		#
		# Due to many-to-one relation between gtfs.trips and gtfs.calendar
		#   (through service_id field), this is done in 3 sequential steps:
		#  - populate gtfs.trips/gtfs.stop_times
		#  - merge calendar timespans for trips where possible
		#  - populate gtfs.calendar/gtfs.calendar_dates, cloning trips/stops where necessary
		#
		# So essentially converting many-to-many relation into many-to-one
		#  with redundant trips, using service_span_trips as a temporary mtm index.
		#
		# XXX: processing of Z schedules
		# XXX: processing of associations

		service_span_trips = collections.defaultdict(set) # {svc_span: trip_id_set}


		## Step-1: Add trips and stop_times for schedules
		#
		# - Only one gtfs.trip_id is created for same
		#   trip_hash (train_uid+stops+stop_times) via trip_merge_idx.
		#
		# - No calendar/service_id entries are created here -
		#   these will be merged in Step-2 via service_span_trips,
		#   and gtfs.trips will be updated with service_id values in Step-3,
		#   duplicating gtfs.trip/gtfs.stop_times for timespans that can't be merged.

		# This limit is for test-runs on small slices only
		schedule_limit = '' if not test_run_slice else f'LIMIT {test_run_slice}'

		route_ids = dict() # {(src, dst}: id}
		trip_merge_idx = dict() # {trip_hash: gtfs.trip_id}

		# XXX: add progress tracking here
		for s in q(f'''
				SELECT *
				FROM cif.schedule s
				LEFT JOIN cif.schedule_extra e ON e.schedule = s.id
				ORDER BY s.train_uid, s.id {schedule_limit}'''):

			# XXX: handle overrides later
			if s.stp_indicator != 'P': continue

			## Get stop sequence information and create gtfs.route_id, ignoring stop times for now
			#
			#  - cif.train_uid does not uniquely identify sequence of gtfs.stops (places),
			#    even for stp_indicator=P schedules only, for example train_uid=W34606
			#  - cif.schedule specifies both stop sequence and their days/times

			stops = list(q('''
				SELECT * FROM cif.stop_time st
				JOIN cif.tiploc t ON t.tiploc_code = st.location
				JOIN gtfs.stops s ON t.crs_code = s.stop_id
				WHERE schedule = %s
				ORDER BY
					public_arrival_time, scheduled_arrival_time,
					public_departure_time, scheduled_departure_time''', s.id))
			if not stops:
				# XXX: there are quite a lot of these - check what these represent
				# self.log.info('Skipping schedule with no usable stops: {}', s)
				continue

			# XXX: note on route_ids? - "how to do routes? get the toc in from schedule_extra"
			# XXX: optimize - merge/split same as services
			route_key = stops[0].id, stops[-1].id
			if route_key in route_ids: route_id = route_ids[route_key]
			else:
				route_id = route_ids[route_key] = insert( 'gtfs.routes',
					route_short_name=f'{stops[0].crs_code}-{stops[-1].crs_code}',
					route_long_name='From {} to {}'.format(*(
						(stops[n].description.title() or stops[n].crs_code) for n in [0, -1] )),
					route_type=int(GTFSRouteType.rail) )

			## Trip and its stops
			#
			#  - Trips are considered equal (via trip_hash) for gtfs purposes
			#    when it's same cif.train_uid and same stops at the same days/times.
			#  - Again, it's not enough to just check train_uid, as schedules
			#    with same cif.train_uid can have different gtfs.stop sequences.

			trip_hash, trip_stops = [s.train_uid], list()
			svc_span = GTFSTimespan(
				s.runs_from, s.runs_to, bank_holidays=bool(s.bank_holiday_running),
				weekdays=dict((k.lower(), getattr(s, k.lower())) for k in calendar.day_name) )

			## Process stop times
			# XXX: time conversions - DTD->GTFS and late->next-day
			for n, st in enumerate(stops, 1):

				public_stop = st.public_arrival_time or st.public_departure_time
				if public_stop:
					pickup_type = GTFSPickupType.regular
					ts_arr, ts_dep = st.public_arrival_time, st.public_departure_time
				else:
					pickup_type = GTFSPickupType.none
					ts_arr, ts_dep = st.scheduled_arrival_time, st.scheduled_departure_time
				# XXX: check if origin has some arrival time indication in CIF data
				ts_arr, ts_dep = ts_arr or ts_dep, ts_dep or ts_arr # origin/termination stops

				trip_stops.append((st, pickup_type, ts_arr, ts_dep))
				trip_hash.append(( st.stop_id,
					ts_arr and ts_arr.total_seconds(), ts_dep and ts_dep.total_seconds() ))

			## Check if such gtfs.trip already exists and only needs gtfs.calendar entry
			trip_hash = hash(tuple(trip_hash))
			if trip_hash in trip_merge_idx: trip_id = trip_merge_idx[trip_hash]
			else:
				trip_id = trip_merge_idx[trip_hash] = len(trip_merge_idx)
				# XXX: check if more trip/stop metadata can be filled-in here
				insert( 'gtfs.trips',
					trip_id=trip_id, route_id=route_id, service_id=s.id,
					trip_headsign=s.train_uid, trip_short_name=s.retail_train_id )
				for st, pickup_type, ts_arr, ts_dep in trip_stops:
					insert( 'gtfs.stop_times',
						trip_id=trip_id,
						arrival_time=ts_arr, departure_time=ts_dep,
						stop_id=st.stop_id, stop_sequence=n,
						pickup_type=int(pickup_type) )
			service_span_trips[svc_span].add(trip_id)


		## Step-2: merge/optimize timespans in service_span_trips where possible
		# XXX: no optimizations implemented yet

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

		trip_svc_id = dict() # {trip_id: svc_id}, XXX: should have "clone or update" flag
		for svc_id, (svc_span, trip_id_set) in enumerate(service_span_trips.items(), 1):
			insert( 'gtfs.calendar', service_id=svc_id,
				start_date=svc_span.start, end_date=svc_span.end, **svc_span.weekdays )
			for trip_id in trip_id_set: trip_svc_id[trip_id] = svc_id


		## Step-3: Assign service_id to gtfs.trips, duplicating where necessary.
		# XXX: clone trip where service inteval was split in two and such

		for trip_id, svc_id in trip_svc_id.items():
			q('UPDATE gtfs.trips SET service_id = %s WHERE trip_id = %s', svc_id, trip_id)



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
	group.add_argument('-n', '--test-schedule-limit', type=int, metavar='n',
		help='Do test-run with specified number of schedules only.'
			' This always produces incorrect results, only useful for testing the code quickly.')
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
	with DTDtoGTFS(opts.src_cif_db, opts.dst_gtfs_db, mysql_conn_opts) as conv:
		conv.run(opts.test_schedule_limit)

if __name__ == '__main__': sys.exit(main())
