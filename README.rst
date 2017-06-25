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

Needs Python-3.6+ and `mysqlclient module <https://mysqlclient.readthedocs.io/>`_.

Simple example: ``./mysql-dtd-to-gtfs.py -v``

As libmysqlclient (via mysqlclient module) is used for db access, most
MySQL-related parameters should be specified for it via e.g. ~/.my.cnf (or
alternative client config, passed as -f/--mycnf-file),
under -g/--mycnf-group section (or default one, if not specified).

Source/destination database names can also be specified via commandline
parameters, as well as a few other logging and debug options.


Links
-----

* `open-track/dtd2mysql <https://github.com/open-track/dtd2mysql>`_

  Parses CIF-format DTD files into MySQL database, to use with this script.
