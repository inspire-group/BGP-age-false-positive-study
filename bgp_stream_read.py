#!/usr/bin/env python
# This file is designed to be included as a general purpose module to read a bgp stream.
# This file is included by test_certs.py and is a core file for the route age study.

from _pybgpstream import BGPStream, BGPRecord, BGPElem
import MySQLdb as mariadb
import time
import json

# Global init section for all code that uses this module.
# Python mysql connect statement
conn = mariadb.connect('localhost', 'routeages', 'routeages', 'routeagescalc',  port=3306)
# SQlite3 connect statement.
#conn = sqlite3.connect('../routeages.db')
cursor = conn.cursor()

# Make the global variables needed but set them to none so that updates and ribs can create two separate streams.
result = None

# create a new bgpstream instance
stream = None
# create a reusable bgprecord instance
rec = None
elem = None

# Define variables that will be read from DB

startTime = 0
endTime = 0
seeded = False

def initUpdates(seedingCheckOverride, lastBGPUpdateProcessed): # Consider passing in last BGP update processed because of new system.
	global cursor, startTime, endTime, seeded, stream, rec
	startTime = lastBGPUpdateProcessed
	#cursor.execute("""SELECT intVal AS lastBGPUpdateProcessed FROM metadata WHERE name = 'lastBGPUpdateProcessed'""")
	#result = cursor.fetchone()
	#if result == None:
	#	print "ERROR: NO Start Time Found in DB (aka lastBGPUpdateProcessed). Recommended snd time is 1489224750 - int(2592000 * 4) (which is 1478856750) because this is a 4 month backlog."
	#	exit()
		#cursor.execute("""INSERT INTO metadata (name, intVal) VALUES ('lastBGPUpdateProcessed', {0})""".format(1489224750 - int(2592000 * 4)))
		#conn.commit()
	#else:
	#	(startTime,) = result
	
	
	cursor.execute("""SELECT intVal AS endTime FROM metadata WHERE name = 'endTime'""")
	result = cursor.fetchone()
	if result == None:
		print "ERROR: NO End Time Found in DB. Recommended end time is 1489224749 because this is the timestamp of the first cert."
		print "ERROR: Other recommended end time is 1491775893 which is when all certs have been covered."
		exit()
	else:
		(endTime,) = result
	
	if not seedingCheckOverride:
		cursor.execute("""SELECT stringVal AS seeded FROM metadata WHERE name = 'seeded'""")
		result = cursor.fetchone()
		if result == None:
			# We will assume that the DB is not seeded if there is no entry and not throw any errors in this case.
			seeded = False
			print "line 59 DB not seeded upon call to initUpdates. DB should be seeded with RIBs before updates can be processed. Call initSeeding first. Exiting."
			exit()
		else:
			strValSeeded = ""
			(strValSeeded,) = result
			if strValSeeded == "false":
				seeded = False
				print "line 66 DB not seeded upon call to initUpdates. DB should be seeded with RIBs before updates can be processed. Call initSeeding first. Exiting."
				exit()
			elif strValSeeded == "true":
				seeded = True
			else:
				print "Invalid value for seeded metadata entry. Exiting."
				exit()
	else:
		seeded = True

	# create a new bgpstream instance
	stream = BGPStream()
	# create a reusable bgprecord instance
	rec = BGPRecord()
	stream.add_filter('record-type','updates') # here we collect updates. This could be changed to ribs to instead acquire periodic snapshots of the RIBs.
	# configure the stream to retrieve Updates from the route-views listener.
	stream.add_filter('collector', 'route-views2')
	# getting updates only from one peer gives us only the perferred route of this peer and no rejected routes.
	# only get updates from level3. Level3 is a provider to ViaWest so their choice is a likely choice of ViaWest.
	stream.add_filter('peer-asn', '3356')
	stream.add_interval_filter(startTime, endTime)
	stream.start()


# This function gives a return value telling if the DB has been seeded.
def initSeeding(lastBGPUpdateProcessed):
	global cursor, startTime, seeded, stream, rec, endTime
	#cursor.execute("""SELECT intVal AS lastBGPUpdateProcessed FROM metadata WHERE name = 'lastBGPUpdateProcessed'""")
	#result = cursor.fetchone()
	#if result == None:
	#	print "ERROR: NO Start Time Found in DB (aka lastBGPUpdateProcessed). Recommended snd time is 1489224750 - int(2592000 * 4) (which is 1478856750) because this is a 4 month backlog."
	#	exit()
		#cursor.execute("""INSERT INTO metadata (name, intVal) VALUES ('lastBGPUpdateProcessed', {0})""".format(1489224750 - int(2592000 * 4)))
		#conn.commit()
	#else:
	#	(startTime,) = result
	startTime = lastBGPUpdateProcessed

	cursor.execute("""SELECT stringVal AS seeded FROM metadata WHERE name = 'seeded'""")
	result = cursor.fetchone()
	if result == None:
		# We will assume that the DB is not seeded if there is no entry and not throw any errors in this case.
		cursor.execute("""INSERT INTO metadata (name, stringVal) VALUES ('seeded', 'false')""")
		conn.commit()
		seeded = False
	else:
		strValSeeded = ""
		(strValSeeded,) = result
		if strValSeeded == "false":
			seeded = False
		elif strValSeeded == "true":
			seeded = True
			print "DB seeded already. Calling initUpdates(True)"
			initUpdates(True, lastBGPUpdateProcessed)
			return True
		else:
			print "Invalid value for seeded metadata entry. Exiting."
			exit()

	# create a new bgpstream instance
	stream = BGPStream()
	# create a reusable bgprecord instance
	rec = BGPRecord()
	stream.add_filter('record-type','ribs') # here we collect updates. This could be changed to ribs to instead acquire periodic snapshots of the RIBs.
	# configure the stream to retrieve Updates from the route-views listener.
	stream.add_filter('collector', 'route-views2')
	# getting updates only from one peer gives us only the perferred route of this peer and no rejected routes.
	# only get updates from level3
	stream.add_filter('peer-asn', '3356')
	stream.add_rib_period_filter(604800)
	stream.add_interval_filter(startTime, startTime + 104800)
	endTime = startTime + 1604801
	stream.start()
	print "bgp stream started from init seeding"
	return False

def primeNextElem():
	global elem, rec, stream
	while (not elem):
		if (not stream.get_next_record(rec)):
			return None
		elem = rec.get_next_elem()
	return elem

def nextUpdateExists():
	global elem, rec
	return (elem is not None) or (primeNextElem())

def fetchNextUpdate():
	global elem, rec, stream
	if (elem):
		res = {'time': rec.time, 'prefix': elem.fields['prefix'] if 'prefix' in elem.fields else None, 
		'type': elem.type, 'as-path': (elem.fields['as-path'] + ' 0') if 'as-path' in elem.fields else None}
		#if elem.peer_asn == 209 or elem.peer_asn == 2828 or elem.peer_asn == 2914 or elem.peer_asn == 3356 or elem.peer_asn == 6939:
		#if elem.peer_asn == 13649:
		#print "getting next bgp update: " + json.dumps(elem.peer_asn)
		elem = rec.get_next_elem()
		#print "[" + str(int(time.time())) + "] bgp update with timestamp: " + str(res['time'])
		return res
	else:
		if nextUpdateExists():
			# Because of the call nextUpdateExists in the if clause, elem is no longer null even though this is the else block where elem was null.
			res = {'time': rec.time, 'prefix': elem.fields['prefix'] if 'prefix' in elem.fields else None, 
			'type': elem.type, 'as-path': (elem.fields['as-path'] + ' 0') if 'as-path' in elem.fields else None}
			elem = rec.get_next_elem()
			#print "bgp update with timestamp: " + str(res['time'])
			return res
		else:
			print "Processed all BGP updates. Exit condition."
			return None


