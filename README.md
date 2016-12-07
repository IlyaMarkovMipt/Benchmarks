# Benchmarks

This is memory test for several databases. There are two types of tests :NoSQL-type and SQL-type. 
And 5 types of bases:
* Redis
* Tarantool
* MongoDB
* Memcached

# Prerequisites
To perform this test you need have databases and python connectors to this bases:
* Tarantool - tarantool-python
* Redis - redis-py
* MongoDB - pymongo
* Memcached - pymemcache
* MySQL - sqlalchemy, python-mysqlconnector


Also you need to have zlib and argparse python libraries

# Usage of test
To perform test you need to use script load.py

  python load.py --help 
  usage: load.py [-h] [--type [TYPE [TYPE ...]]] [--bases [BASES [BASES ...]]]

  Memory benchmarks

  optional arguments:
    -h, --help            show this help message and exit
    --type [TYPE [TYPE ...]], -t [TYPE [TYPE ...]]
                          type: S(SQL) or N(Nosql)
    --bases [BASES [BASES ...]], -b [BASES [BASES ...]]
                          database: T(Tarantool), My(Mysql), R(Redis),
                          Me(Memcached), Mg(MongoDB), A(All)


