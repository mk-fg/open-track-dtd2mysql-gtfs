open-track-dtd2mysql-gtfs
=========================

Converter for `open-track/dtd2mysql <https://github.com/open-track/dtd2mysql>`_
parsed UK DTD Timetable schema to GTFS schema in MySQL database (or its forks).

Basically, given database with schema from `doc/db-schema-cif.sql <doc/db-schema-cif.sql>`_,
as produced by dtd2mysql, populates database with `GTFS schema <doc/db-schema-gtfs.sql>`_.

This project is a prototype, implementing all necessary conversion steps for
later reference, i.e. more of a proof of concept than production code.

Under heavy development, nothing works yet.

.. contents::
  :backlinks: none


Usage
-----

Needs Python-3.6+ (or pypy3-5.8.0+) and
`mysqlclient module <https://mysqlclient.readthedocs.io/>`_.

Simple example: ``./mysql-dtd-to-gtfs.py -v``

As libmysqlclient (via mysqlclient module) is used for db access, most
MySQL-related parameters should be specified for it via e.g. ~/.my.cnf (or
alternative client config, passed as -f/--mycnf-file),
under -g/--mycnf-group section (or default one, if not specified).

Source/destination database names can also be specified via commandline
parameters, as well as a few other logging and debug options.

Running script with --debug flag will make it log progress messages,
elapsed/estimated time and some stats::

  % ./mysql-dtd-to-gtfs.py --debug -n100
  2017-06-27 07:37:11 :: dtd2gtfs DEBUG :: Processing 414 cif.schedule entries...
  2017-06-27 07:37:11 :: dtd2gtfs DEBUG :: [schedules] Step  0 / 30: 00.00s trips=0
  2017-06-27 07:37:11 :: dtd2gtfs DEBUG :: [schedules] Step  1 / 30: 00.02s trips=5
  2017-06-27 07:37:11 :: dtd2gtfs DEBUG :: [schedules] Step  2 / 30: 00.01s trips=8
  ...
  2017-06-27 07:37:12 :: dtd2gtfs DEBUG :: [schedules] Step 29 / 30: 01.00s trips=135
  2017-06-27 07:37:12 :: dtd2gtfs DEBUG :: Merging service timespans for 141 gtfs.trips...
  2017-06-27 07:37:12 :: dtd2gtfs DEBUG :: Updating service_id in gtfs.trips table...
  2017-06-27 07:37:13 :: dtd2gtfs DEBUG :: [LWX3] Stats:
  2017-06-27 07:37:13 :: dtd2gtfs DEBUG :: [LWX3]   sched-count: 414
  2017-06-27 07:37:13 :: dtd2gtfs DEBUG :: [LWX3]   sched-entry-C: 58
  ...
  2017-06-27 07:37:13 :: dtd2gtfs DEBUG :: [LWX3]   trip-dedup: 91
  2017-06-27 07:37:13 :: dtd2gtfs DEBUG :: [LWX3]   trip-row-dup: 22
  2017-06-27 07:37:13 :: dtd2gtfs DEBUG :: [LWX3]   trip-row-dup-op: 43

(-n/--test-train-limit option used here allows to randomly select and process
schedules for only n trains instead of full database, useful for quick test-runs)


Links
-----

* `open-track/dtd2mysql <https://github.com/open-track/dtd2mysql>`_

  Parses CIF-format DTD files into MySQL database, to use with this script.
