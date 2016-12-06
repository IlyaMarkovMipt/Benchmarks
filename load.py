import random
import string
import json
import os.path
import pandas as pd

#schema of object is folowing 
#
#{
#   name: string
#   coords: array of numbers
#}

FILE_JSON="objects.json"
num_objects = 200000
strings = set()

def generate_objects(num_objects):
    objects = []
    for i in xrange(num_objects):
        len_name = random.randint(2, num_objects/100)
        len_coords = random.randint(5, 50)
        objects.append(generate_object(i, len_name, len_coords))
        if i % 1000 == 0:
            print i
    return objects

def generate_object(id_, len_name, len_coords):
    object_ = {}
    object_["name"] = str(generate_string(len_name))
    object_["coords"] = [random.uniform(-180, 180) for i in xrange(len_coords)]
    return object_

def generate_string(size=32):
    chars = string.ascii_lowercase + string.digits
    s = ''.join(random.choice(chars) for x in range(size))
    while s in strings:
        s = ''.join(random.choice(chars) for x1 in range(size))
    strings.add(s)
    return s

def redis_load_nosql(objects, numbers):
    import redis
    print "redis nosql size(without indexes):"
    r = redis.Redis(host='localhost', port=6379, db=0)

    test_name = "redis nosql"
    size = 0
    for i, o in enumerate(objects):
        #for f in o["coords"]:
           # r.rpush(o["name"], f)
        r.set(o["name"], o["coords"])
        val = r.get(o["name"])
        if not val:
            print test_name + "empty response"
            print "original " + str(o)
            break
        if not val != [str(x) for x in o["coords"]]:
            print test_name + " response is not equal to original"
            print "original " + str(o["coords"])
            print "return " + str(val)
            break

        row = r.debug_object(o["name"])
        size += row["serializedlength"]
        if (i + 1) in numbers:
            print test_name + " objects: %s size: %s MB" % (i  +1, size * 1.0 / (1024*1024))

def redis_load_sql(df):
    import redis
    import zlib
    r = redis.Redis(host='localhost', port=6379, db=0)

    size = 0
    test_name = "redis sql"
    for i, row in enumerate(df.values):
        l = list(row)
        # l[11] = zlib.compress(l[11])
        r.set(i, l)
        # val = r.get(id)
        # if not val:
        #     print test_name + "empty response"
        #     print "original " + str(l)
        #     break
        # s = '[' + ', '.join(map(lambda x: '"' + x + '"' if x is str else str(x), l)) + ']'
        # if val != s:
        #     print type(val)
        #     print test_name + " response is not equal to original"
        #     print "original " + s
        #     print "return " + str(val)
        #     break
        info = r.debug_object(i)

        size += info["serializedlength"]
    print "size of keys: %s" % r.dbsize()
    print test_name + " size(without indexes): %s MB" % (size * 1.0 / (1024 * 1024))



def tarantool_load_nosql(objects, numbers):
    import tarantool

    c = tarantool.connect("localhost", 3301)
    
    space = c.space(513)

    test_name = "tarantool nosql"

    for i, o in enumerate(objects):
        tup = (o['name'], o['coords'],)
        c.call('insert_hash', tup[0], tup[1])
        response = c.call('my_get', tup[0])
        if tuple(response[0]) != tup:
            print test_name + " is not equal to original"
            print "original " + str(tup)
            print "return " + str(response[0])
            break
        if (i + 1) in numbers:
            print test_name + " objects: %s size: %s MB" % (i + 1, c.call("mem_used", 513)[0][0])


def tarantool_load_sql(df):
    import tarantool
    import zlib

    c = tarantool.connect("localhost", 3301)

    space = c.space(514)

    test_name = "tarantool sql"

    for i, row in enumerate(df.values):
        tuple1 = (i,) + tuple(row)
        cmpd = zlib.compress(row[11])
        tup = (i,) + tuple(row[:11]) + (cmpd, row[12])
        space.replace(tup)
        response = space.select(i)
        if not response:
            print test_name + "empty response"
            print "original " + str(tup)
            break
        #response[0][12] = zlib.decompress(response[0][12])
        # if tuple1 not in [tuple(response[0])]:
        #     print test_name + " response is not equal to original"
        #     print "original " + str(tuple1)
        #     print "return " + str(response[0])
        #     break
    print test_name + " size: %s MB" % (c.call("mem_used", 514)[0][0])


def mysql_load_nosql(objects, numbers):
    import mysql.connector
    cnx = mysql.connector.connect(user='ilya', password='password', host='127.0.0.1', database='footprints')
    cursor = cnx.cursor()
    #print "Mysql size:"

    table_name = "my_coords"
    test_name = "mysql nosql"
    add_record = ("INSERT INTO my_coords "
                   "VALUES (\'%s\')")

    from time import sleep

    for i, o in enumerate(objects):
        record = json.dumps(o)
        #print
        cursor.execute(add_record % record)
        if (i + 1) in numbers:
            cnx.commit()
            sleep(10)
            cursor.execute("SHOW TABLE STATUS LIKE '%s'" % table_name)
            l = cursor.fetchone()
            print test_name + " objects: %s size: %s MB" % (i + 1, (l[6] + l[8]) * 1.0 / (1024 * 1024))
    cursor.close()
    cnx.close()

def mysql_load_sql(df):
    from sqlalchemy import create_engine
    from BeerClass import Beer
    from sqlalchemy.orm import sessionmaker

    print "sqlalchemy starts"
    engine = create_engine("mysql+mysqlconnector://ilya:password@127.0.0.1:3306/footprints")
    # coltype = {'time': DATETIME}
    # df.to_sql('beer', engine, if_exists='replace', dtype=coltype)

    test_name = "mysql sql test"
    session = sessionmaker()
    session.configure(bind=engine)
    Beer.metadata.create_all(engine)

    table_name = Beer.__table__

    s = session()
    print "start inserting"
    s.commit()

    # c = 0
    for row in df.values:
        #print c
        #c += 1
        beer = Beer(row=row)
        s.add(beer)
    s.commit()

    import mysql.connector
    cnx = mysql.connector.connect(user='ilya', password='password', host='127.0.0.1', database='footprints')
    cursor = cnx.cursor()

    cursor.execute("SHOW TABLE STATUS LIKE '%s'" % table_name)
    l = cursor.fetchone()

    print test_name + " size: %s MB" % ((l[6] + l[8]) * 1.0 / (1024 * 1024))

    # for i, row in enumerate(df.values):
    #     brewer_id = s.query([Beer.brewer_id]).filter(Beer.id == i).one()
    #     if brewer_id != row[2]:
    #         print "Id are not equal orig: %s, return: %s" % (brewer_id, row[2])



def mongo_load_nosql(objects, numbers):
    import pymongo
    import hashlib
    from  bson.binary import Binary
    from time import sleep

    client = pymongo.MongoClient()
    db = client.test
    test_name = 'mongo nosql '
    pred_number = 0
    for object in objects:
        object["name"] = Binary(hashlib.sha1(object["name"]).hexdigest())
    for number in numbers:
        result = db.nosql.insert_many(objects[pred_number:number])
        if number < len(objects) and len(result.inserted_ids) != number - pred_number:
            print "size %s is not equal as expected %s" %(len(result.inserted_ids), number - pred_number)
            break
        pred_number = number
        # cursor = db.nosql.find({"name": object["name"]})
        # if cursor[0]["coords"] != object["coords"]:
        #     print test_name + "Response not equal to original"
        #     print "Original: " + str(object)
        #     print "Response: " + str(cursor)
        #     break
        #
        res = db.command("collstats", "nosql")
        size = res["size"] + res["totalIndexSize"] + 16 * res["count"] # last one is header
        print test_name + "objects: %s, size: %s MB" % (res["count"], size * 1.0 / (1024 * 1024))

def mongo_load_sql(df):
    import pymongo
    import zlib
    from bson.binary import Binary
    client = pymongo.MongoClient()
    db = client.test2
    df['text'] = df['text'].map(zlib.compress).map(Binary)
    d = df.to_dict(orient='records')
    batch_size = 4000
    pred = 0
    cur = batch_size
    test_name = 'mongo sql '

    while pred < len(d):
        result = db.nosql.insert_many(d[pred:cur])
        if len(result.inserted_ids) != cur - pred:
            print "size %s is not equal as expected %s" % (len(result.inserted_ids), cur - pred)
            break
        pred = cur
        cur += batch_size

    res = db.command("collstats", "nosql")
    size = res["size"] + res["totalIndexSize"] + 16 * res["count"]  # last one is header
    print test_name + "objects: %s, size: %s" % (res["count"], size * 1.0 / (1024 * 1024))


def mem_serializer(key, value):
    return json.dumps(value), 1

def mem_deserializer(key, value, flags):
    return json.loads(value)

def memcached_load_nosql(objects, numbers):
    from pymemcache.client.base import Client
    import hashlib

    mc = Client(('localhost', 11211), serializer=mem_serializer, deserializer=mem_deserializer)
    test_name = 'memcached nosql '

    for i, o in enumerate(objects):
        mc.set(str(hashlib.sha1(o["name"]).hexdigest()), o["coords"])
        if (i + 1) in numbers:
            print test_name + "Objects: %s, Usage: %s" % (i + 1, mc.stats()["bytes"] * 1.0 / (1024 * 1024))
    print "finsihed"

def memcached_load_sql(df):
    from pymemcache.client.base import Client
    #import zlib
    mc = Client(('localhost', 11211))#, serializer=mem_serializer, deserializer=mem_deserializer)
    #from bson.binary import Binary

    #df['text'] = df['text'].map(zlib.compress)#.map(Binary)

    test_name = 'mongo sql '
    d = df.to_dict(orient='records')
    for i, row in enumerate(df.values):
        mc.set(str(i), str(d[i]))

    print test_name + "Usage: %s" % (mc.stats()["bytes"] * 1.0 / (1024 * 1024))

def couch_load_nosql(objects, numbers):
    import couchdb
    server = couchdb.Server()



def couch_load_sql(df):
    pass

def generate_and_save():
    with open(FILE_JSON, 'w') as out:
        json.dump(generate_objects(num_objects), out, separators=(',', ':'))

def nosql_type_test():
    if not os.path.isfile(FILE_JSON):
        generate_and_save()
    with open(FILE_JSON, 'r') as in_put:
        objects = json.load(in_put)

    numbers = [10000, 20000, 50000, 100000, 200000]
    # tmp_objects = []
    # import pickle
    # for i, object in enumerate(objects):
    #     tmp_objects.append(object)
    #     if (i + 1) in numbers:
    #         name = "nosql" + str(i) + ".pickle"
    #         with open(name, 'wb') as out:
    #             pickle.dump(tmp_objects, out)
    #         print "Size pickle file %s: %s Mb" % (name ,os.path.getsize(name) * 1.0 / (1024 * 1024))
    # tarantool_load_nosql(objects, numbers)
    # mongo_load_nosql(objects, numbers)
    # redis_load_nosql(objects, numbers)
    # mysql_load_nosql(objects, numbers)
    memcached_load_nosql(objects, numbers)


sql_funcs = [tarantool_load_sql, redis_load_sql, mongo_load_sql, memcached_load_sql, mysql_load_sql]

def sql_type_test(bases):
    BEER_FILE = "beer_subset.csv.gz"
    if not os.path.isfile(BEER_FILE):
        raise Exception("BEER not found")
    
    df = pd.read_csv('beer_subset.csv.gz', parse_dates=['time'], compression='gzip', encoding='utf-8')
    df.dropna(inplace=True)
    tmp_time = df["time"]
    # import pickle
    # with open("sql.pickle", 'wb') as out:
    #     pickle.dump(df.to_dict(), out)
    print "Size pickle file sql %s Mb" % (os.path.getsize("sql.pickle") * 1.0 / (1024 * 1024))
    for b in bases:
        if b == 4:
            df["time"] = tmp_time
            df["beer_name"] = "a" * 21
            sql_funcs[b](df)
        else:
            tmp_time = df["time"]
            df["time"] = df["time"].astype(int)
            df["text"] = df["text"].astype(str)
            sql_funcs[b](df)

import argparse

def main():
    parser = argparse.ArgumentParser(description="Memory benchmarks")
    parser.add_argument("type" ,"--type", nargs='*', type=str ,help="type: S(SQL) or N(Nosql)")
    parser.add_argument("bases", "--base", nargs='*',
                        help="database: T(Tarantool), My(Mysql), R(Redis), Me(Memcached), Mg(MongoDB), A(All)")
    args = parser.parse_args()
    funcs = []

    if args.type.tolowercase().contains('s'):
        funcs.append(sql_type_test)
    if args.type.tolowercase().contains('n'):
        funcs.append(nosql_type_test)

    bases = []
    s = args.bases.tolowercase()
    if s.contains('a'):
        bases = [x for x in range(5)]
    else:
        if s.contains('t'):
            bases.append(0)
        if s.contains('r'):
            bases.append(1)
        if s.contains('Mg'):
            bases.append(2)
        if s.contains('Me'):
            bases.append(3)
        if s.contains('My'):
            bases.append(4)
    if not funcs:
        print "specify type of test"
    for f in funcs:
        f(bases)

if __name__ == "__main__":
    main()
