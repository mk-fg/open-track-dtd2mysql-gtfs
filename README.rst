open-track-dtd2mysql-gtfs
=========================

Misc tools to convert UK railways timetable data (CIF dumps from
`ATOC website <http://data.atoc.org/data-download>`_) to
`GTFS feed <https://developers.google.com/transit/gtfs/reference/>`_,
as well as validate and compare resulting GTFS data.

In particular:


- mysql-dtd-to-gtfs.py

  Converts parsed CIF data (stored in MySQL by
  `open-track/dtd2mysql <https://github.com/open-track/dtd2mysql>`_)
  to GTFS feed.

  | Source data schema (as created by dtd2mysql): `doc/db-schema-cif.sql <doc/db-schema-cif.sql>`_
  | GTFS schema: `doc/db-schema-gtfs.sql <doc/db-schema-gtfs.sql>`_

  Requirements:
  Python-3.6+ or PyPy3-5.8.0+,
  `pymysql module <https://pymysql.readthedocs.io/>`_.


- gtfs-compare.py

  Compares two GTFS feeds (importing into MySQL if necessary),
  showing any timetable differences between them.

  Designed to compare output between different GTFS converter implementations,
  e.g. mysql-dtd-to-gtfs.py from this repo and one from dtd2mysql.

  Requirements:
  Python-3.6+ or PyPy3-5.8.0+,
  `pymysql <https://pymysql.readthedocs.io/>`_,
  `deepdiff <http://deepdiff.readthedocs.io/>`_.


- gtfs-webcheck.py

  Tool to query/scrape online timetable sources / journey planners and compare
  trips from there with GTFS data, to highlight any potential bugs in CIF-GTFS
  export code.

  Requirements:
  Python-3.6+,
  `aiohttp <http://aiohttp.readthedocs.io/>`_,
  `aiomysql <http://aiomysql.readthedocs.io/>`_.


For all scripts, most MySQL-related parameters should be specified via
the usual ~/.my.cnf (or alternative client config, passed as -f/--mycnf-file),
under -g/--mycnf-group section (or default one, if not specified).

Source/destination database names should be supplied on the command line, see
``-h/--help`` output for more details.

.. contents::
  :backlinks: none


Usage examples
--------------

- Parse CIF timetable zip from ATOC website to MySQL database using dtd2mysql::

    % npm install dtd2mysql

    % export DATABASE_HOSTNAME=10.1.0.2 DATABASE_USERNAME=cif DATABASE_PASSWORD=sikrit DATABASE_NAME=cif
    % dtd2mysql --fares RJFAF499.ZIP
    % dtd2mysql --fares-clean
    % dtd2mysql --timetable ttis625.zip

  (see `open-track/dtd2mysql <https://github.com/open-track/dtd2mysql>`_ for more info)

- Convert imported CIF data to GTFS feed (stored in mysql db)::

    % pip3 install --user pymysql

    % cat >my.cif.cnf <<EOF
    [client]
    default-character-set=utf8mb4
    host=10.1.0.2
    user=cif
    password=sikrit
    EOF

    % ./mysql-dtd-to-gtfs.py --debug -i -f my.cif.cnf -s cif -d gtfs_py

  Using PyPy instead of regular CPython will speed up the process dramatically.

- Use dtd2mysql to produce GTFS feed as txt files in current dir::

    % dtd2mysql --gtfs

  (using same DATABASE_* parameters from env)

- Import GTFS feed from txt files in current dir (".") into MySQL schema,
  find and display differences between it and gtfs_py feed/db::

    % pip3 install --user deepdiff

    % ./gtfs-compare.py --debug import -i -- . gtfs_ts
    % ./gtfs-compare.py compare -c cif -- gtfs_py gtfs_ts

  (``-c cif`` will allow script to augment produced diffs with info from
  e.g. CIF schedule entries, to cross-reference with GTFS data)

- Test GTFS data from gtfs_py db against online sources::

    % pip3 install --user aiohttp aiomysql

    % ./gtfs-webcheck.py --debug -d gtfs_py -n10
    % ./gtfs-webcheck.py --debug -d gtfs_py -u C04612

  ``-n10`` will limit testing to 10 arbitrary trains, ``-u C04612`` checks
  specific train_uid (matching it from gtfs trip_headsign field).

  For longer runs, see --trip-id-log/--diff-log parameters, rate-limiting, as
  well as other configuration params, most of which are only available in
  TestConfig at the top of the script itself at the moment.


Links
-----

* `ATOC website <http://data.atoc.org/data-download>`_

  CIF data downloads for UK railways timetable/fares, requires (free) registration.

* `open-track/dtd2mysql <https://github.com/open-track/dtd2mysql>`_

  | Parses CIF-format DTD files into MySQL database, to use with this script.
  | Also has CIF->GTFS exporter implementation under "gtfs" branch.
