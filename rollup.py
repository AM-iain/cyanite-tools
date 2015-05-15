#!/usr/bin/python

import os
import sys
import time
import yaml

if len(sys.argv) > 1:
  timestamp = int(sys.argv[1])
  argv = sys.argv[2:]
else:
  argv = ['bogus']

rollups = []
now = time.time()
if len(argv):
  while len(argv):
    if len(argv) > 1:
      rollups.append([int(argv[0]), int(argv[1])])
      argv = argv[2:]
    else:
      print "Usage: %s <timestamp> [[<period> <rollup>] ...]" % sys.argv[0]
      sys.exit(1)
else:
  try:
    config_file = os.getenv('CYANITE_CONFIGURATION')
    if config_file is not None:
      yaml = yaml.load(open(config_file, 'r'))
    else:
      yaml = yaml.load(open('/opt/graphite/conf/cyanite.yaml', 'r'))
    for rollup in yaml['carbon']['rollups']:
      rollups.append([rollup['period'], rollup['rollup']])
  except Exception, e:
    print "Can't read rollups from YAML."
    sys.exit(111)

for r in rollups:
  period, rollup = r
  rounded = int(timestamp / rollup) * rollup
  output = 'period: %6d; rollup: %3d; time: %d' % (period, rollup, rounded)
  if timestamp >= now - (period * rollup):
    print output
  else:
    newest = int(now / rollup) * rollup
    oldest = newest - (period * rollup)
    print '%s OUTSIDE RANGE [%d, %d]' % (output, oldest, newest)
