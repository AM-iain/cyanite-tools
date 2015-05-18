#!/usr/bin/python

# apt-get install python-blist
# pip install cassandra-driver
from cassandra.query import dict_factory
from cassandra.cluster import Cluster
# apt-get install python-dateutil
from dateutil.parser import parse
# pip install elasticsearch
from elasticsearch import Elasticsearch
from getopt import gnu_getopt
from os import getenv
import re
import string
import sys
from time import localtime, strftime, time, mktime
# apt-get install python-yaml
import yaml

try:
  config_file = getenv('CYANITE_CONFIGURATION')
  if config_file is not None:
    yaml = yaml.load(open(config_file, 'r'))
  else:
    yaml = yaml.load(open('/opt/graphite/conf/cyanite.yaml', 'r'))
  store = yaml['store']
  clustername = store['cluster']
  keyspace = store['keyspace']
  metric = store['cluster']
  elasticsearch = yaml['index']
  index = elasticsearch['index']
  url = elasticsearch['url']
  rollups = []
  for rollup in yaml['carbon']['rollups']:
    rollups.append([rollup['period'], rollup['rollup']])
except Exception, e:
  if config_file is not None:
    print 'Failed to read YAML from "%s"' % config_file
    sys.exit(1)
  print 'Using defaults...'
  clustername = 'localhost'
  keyspace = 'metric'
  index = 'cyanite_paths'
  url = 'http://localhost:9200'
  rollups = [
      [60480, 10],
      [105120, 600]
  ]

def glob(patterns):
  es = Elasticsearch()
  ret = []
  for pattern in patterns:
    if re.search('[\*\?{}]', pattern):
      qtype = 'regexp'
      pattern = string.replace(pattern, '*', '[^.]*')
      pattern = string.replace(pattern, '?', '.')
      while True:
        m = re.search('\{(.*)\}', pattern)
        if m is None:
          break
        inside = string.replace(m.group(1), ',', '|')
        pattern = string.replace(pattern, m.group(0), '(%s)' % inside)
        break
    else:
      qtype = 'term'
    scroll = '2m'
    size = 1000

    body = { 'query': { 'bool': { 'must': [ { qtype: { 'path': pattern } }, { 'term': { 'leaf': 'true' } } ] } } }
    result = es.search(index=index, doc_type='path', scroll=scroll, size=size, body=body)
    scroll_id = result['_scroll_id']
    total = result['hits']['total']
    hits = result['hits']['hits']

    while len(hits):
      for hit in hits:
        ret.append(hit['_id'])
      result = es.scroll(scroll_id=scroll_id, scroll=scroll)
      scroll_id = result['_scroll_id']
      hits = result['hits']['hits']
  return ret

def parse_timestamp(string):
  if re.search('^\d+$', string):
    return int(string)
  elif string == 'now':
    return time()
  else:
    return mktime(parse(string, fuzzy = True).timetuple())

def best_rollup(since, rollups):
  now = time()
  for candidate in sorted(rollups):
    period, rollup = candidate
    if since >= now - (period * rollup):
      return candidate
  return [None, None]

optlist, argv = gnu_getopt(sys.argv[1:], ':f:l:t:v')
opts = {}
for opt, arg in optlist:
  opts[re.sub('^-*(.).*', '\g<1>', opt)] = arg

verbose = 'v' in opts

if 't' in opts:
  until = parse_timestamp(opts['t'])
else:
  until = time()

if 'f' in opts:
  since = parse_timestamp(opts['f'])
else:
  since = until - 3600

if verbose:
  print 'Earliest point:\n%s (%d)' % (strftime('%Y-%m-%d %H:%M:%S', localtime(since)), since)
  print 'Latest point:\n%s (%d)' % (strftime('%Y-%m-%d %H:%M:%S', localtime(until)), until)

if 'l' in opts:
  limit = int(opts['l'])
else:
  limit = 10

tenant = u''
if len(argv):
  if verbose:
    print 'Elasticsearch: %s/%s' % (url, index)
  paths = glob(argv)
  if len(paths):
    period, rollup = best_rollup(since, rollups)
    cql = """  select path, data, time
    from metric
   where tenant='%s'
     and period=%s
     and rollup=%s
     and path in ('%s')
     and time >= %d and time <= %d
   limit %d""" % (tenant, period, rollup, "','".join(paths), since, until, limit)
    if verbose:
      print 'Best rollup:\n%s' % [period, rollup]
      print 'Cluster: %s' % clustername
      print 'Keyspace: %s' % keyspace
      print "Query:\n%s;" % cql
    cluster = Cluster([clustername])
    session = cluster.connect(keyspace)
    session.row_factory = dict_factory
    rows = session.execute(cql)
    for row in sorted(rows, key=lambda x: x['time']):
      print '%s %s %s' % (strftime('%Y-%m-%d %H:%M:%S', localtime(row['time'])), row['path'], row['data'])
  else:
    print 'No metrics matching "%s" found in Elasticsearch!' % '", "'.join(argv)
    print 'Which does NOT necessarily mean they don\'t exist in Cassandra...'
else:
  print 'Usage: %s [-v] [-f <from>] [-t <to>] [-l <limit>] <glob> [<glob> ...]' % sys.argv[0]
