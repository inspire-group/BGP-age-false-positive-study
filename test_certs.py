#!/usr/bin/python
# This is the core file for the certificate age study.
# Includes bgp_stream_read and read_certificate_history.


from bgp_stream_read import nextUpdateExists, fetchNextUpdate, primeNextElem, initSeeding, initUpdates
from read_certificate_history import getNextCertificate
#import mysql.connector as db
import MySQLdb as db
#import sqlite3
import time
import socket
import netaddr
import sys
import os.path


scriptPath = os.path.dirname(os.path.realpath(__file__))




#mysql.connector version
#conn = db.connect(host='localhost', user='routeages', password='routeages', database='routeages', port=3306)

# Mysql-python version 
conn = db.connect('localhost', 'routeages', 'routeages', 'routeagescalc',  port=3306)

# sqlite version
#conn = sqlite3.connect('../routeages.db')
cursor = conn.cursor()

# New DB structure requires an original start time but lets lastBGPUpdate be optional.

# This code assumes end time is already in the Database.
endTime = 0
cursor.execute("""SELECT intVal AS endTime FROM metadata WHERE name = 'endTime'""")
(endTime,) = cursor.fetchone()

# This code assumes original start time is already in the Database.
originalStartTime = 0
cursor.execute("""SELECT intVal AS originalStartTime FROM metadata WHERE name = 'originalStartTime'""")
(originalStartTime,) = cursor.fetchone()


def pad16(number): #pads a number to be 16 bytes of decimal ASCII.
	return "{0:016d}".format(number)

lastBGPUpdateProcessedFileLocation = scriptPath + "/last-BGP-update-processed.var"

if not os.path.isfile(lastBGPUpdateProcessedFileLocation) or os.stat(lastBGPUpdateProcessedFileLocation).st_size == 0:
	with open(lastBGPUpdateProcessedFileLocation, 'wb') as lastBGPUpdateProcessedFile:
		# If the file is empty, write the original start time in so we can start at the beginning.
		lastBGPUpdateProcessedFile.write(pad16(originalStartTime))#The file is 16 base 16 digits long.

# These two variables are the same.
startTime = 0
lastBGPUpdateProcessed = 0
with open(lastBGPUpdateProcessedFileLocation, 'rb') as lastBGPUpdateProcessedFile:
	lastBGPUpdateProcessed = int(lastBGPUpdateProcessedFile.read())

startTime = lastBGPUpdateProcessed

seeded = initSeeding(lastBGPUpdateProcessed)

routeAges = []

lastCertificateIndexProcessedFileLocation = scriptPath + "/last-certificate-index-processed.var"
# Because we did not use OO programing where an object could clean up its own mess, we need to open the file for the lastCertificateIndexProcessedFile here and pass it into the lower functions.
with open(lastCertificateIndexProcessedFileLocation, 'wb') as lastCertificateIndexProcessedFile:
	with open(lastBGPUpdateProcessedFileLocation, 'wb') as lastBGPUpdateProcessedFile:
		# Replace original info in file so it doesn't look blank if progrm exits before any BGP processing takes place.
		lastBGPUpdateProcessedFile.write(pad16(startTime))
		def updateLastBGPUpdateProcessed(time):
			global lastBGPUpdateProcessedFile
			lastBGPUpdateProcessedFile.seek(0)
			lastBGPUpdateProcessedFile.write(pad16(time))

		def processCertificate():
			global currentCRT, cursor, crtTimeStr, crtTime
			
			routeAge = {}
			#ipString = socket.gethostbyname(currentCRT['commonName']) #before loops were done in this script. The Cert Database contains the ips now.
			ipString = currentCRT["resolvedIP"]
			if ipString == None:
				routeAge = {"commonName": currentCRT['commonName'], "hopAge": "-2"}
			else:
				for i in range(24,7,-1):
					cidr = netaddr.IPNetwork(str(ipString) +"/" + str(i)).cidr
					cursor.execute("""SELECT timeList, asPath, previousASPath, addedTime FROM bgpPrefixUpdates WHERE prefix='{0}'""".format(str(cidr)))
					result = cursor.fetchone()
			
					#if this network is in the database, store the result
					if result:
						(storedTimeList, asPath, previousASPath, addedTime) = result

						if not asPath:
							continue

						effectiveAddedTime = 0
						# In this version of the script we are only considering added time if the current AS path exactly matches the one from the previous annoucement.
						if asPath == previousASPath:
							effectiveAddedTime = addedTime

						storedTimeListArray = storedTimeList.split(' ')
			
						deltaTimeListArray = storedTimeList.split(' ')
			
						for i in xrange(len(storedTimeListArray)):
							if storedTimeListArray[i] == '-3':
								#print "-3 case"
								# Handle routes that are in the RIBs already.
								deltaTimeListArray[i] = '-3'
							else:
								deltaTimeListArray[i] = str(crtTime - int(storedTimeListArray[i]) + effectiveAddedTime)
			
						deltaTimeList = ' '.join(deltaTimeListArray)
						routeAge = {"commonName": currentCRT['commonName'], "hopAge": deltaTimeList, "resolvedPrefix": str(cidr), "resolvedAsPath": asPath}
						break
		
				#if the for loop ends with something other than the break statement the route was not in the database.
				if routeAge == {}:
					routeAge = {"commonName": currentCRT['commonName'], "hopAge": "-1"}
			#-1: Route not in DB -2: IP not resolved from certificate -3:Hop in RIBs not updates
			#Since the blow comment -3 was also added as a majic value. Unlike -1 (route not in DB) and -2 (ip not found) -3 is used in place of an individual hop age and means that that hop was found in the original ribs not in one of the updates.
			#It should be noted that -1 and -2 for hop age strings are magic values. -1 Implies that the ip address was resolved but the route was not in the database meaning that it is a route older than the period we populated the database with.
			#-2 indicates that the domain name no longer resolves to an IP. This is an onfortunate case because we have no clue how old the route is.
			#the -2 error also highlights the imperfections of these methods, between when Let's Encrypt actually issued the cert and when we run this script the domain name could eaily have changed.
			#unfortunatly I can not think of any way to handle this at the moment. We would require access to some sort of historical DNS record.
			#DNS Trails seems to have this type of functionality, but it only supports a small number of top level domains so it might not be worth implementing.
			#Also, DNS is location speciffic so to be rigurious we would have to also know what DNS answer Let's Encrypt speciffically got at that time.
			#print {"timeOfCertificateIssuing": crtTime, "rateAgeInfo": routeAge}
			#routeAges.append(routeAge)
			# INSERT IGNORE is used to not overwrite data from previously processed certificates. If the certificate was previously processed we are most likely processing it at the wrong moment in simulated time and the original entry is more accurate.
			# Processing time is a trap. It is not a meaningful value. It is the time that the python post-processing script was run with historical data. It is not a very relavant piece of information.
			cursor.execute("""INSERT IGNORE INTO routeages (certSqlId, ages, processingTime, resolvedPrefix, resolvedAsPath) VALUES 
				({0}, '{1}', {2}, {3}, {4})""".format(currentCRT["sqlId"], routeAge["hopAge"], int(time.time()),
				"'" + routeAge["resolvedPrefix"] + "'" if "resolvedPrefix" in routeAge else "null",
				"'" + routeAge["resolvedAsPath"] + "'" if "resolvedAsPath" in routeAge else "null"))
			conn.commit()
		
			currentCRT = getNextCertificate(lastCertificateIndexProcessedFile)
			if (not currentCRT):
				exit()
			crtTime = currentCRT['timestamp']
			#crtTime = int(calendar.timegm(dateparser.parse(crtTimeStr, settings={'TO_TIMEZONE': 'UTC'}).timetuple()))
			
		
		currentCRT = getNextCertificate(lastCertificateIndexProcessedFile)
		if (not currentCRT):
				exit()
		crtTime = currentCRT['timestamp']
		#crtTime = time.mktime(time.strptime(crtTimeStr[:-4], "%Y-%m-%d %H:%M:%S"))
		
		
		# Record the time the program began execution.
		programStartTime = time.time()
		
		def masterUpdateLoop():
			printCounter = 0
			while (nextUpdateExists()):
				update = fetchNextUpdate()
				if seeded:
					while update['time'] > crtTime:
						processCertificate()
				
				if update['type'] == 'A' or update['type'] == 'R':
			
					#asPath = conn.escape_string(update['as-path'])
					asPath = update['as-path']
					asPathLength = asPath.count(' ') + 1
					#prefix = conn.escape_string(update['prefix'])
					prefix = update['prefix']
					# Seeding could probably go faster if this query was not being executed and ignored. 
					cursor.execute("""SELECT asPath, timeList FROM bgpPrefixUpdates WHERE prefix='{0}'""".format(prefix))
					result = cursor.fetchone()
					if result == None or (not seeded):
						timeList = ' '.join([str(update['time'])] * asPathLength)
						# Here R stands for a RIB update meaning we are in seeding mode.
						if update['type'] == 'R':
							timeList = ' '.join(['-3'] * asPathLength)
						# Mysql version
						cursor.execute("""INSERT INTO bgpPrefixUpdates (prefix, asPath, timeList, updateTime) VALUES ('{0}', '{1}', '{2}', {3}) 
							ON DUPLICATE KEY UPDATE asPath='{4}', timeList='{5}', 
							updateTime={6}""".format(prefix, asPath, timeList, int(update['time']), asPath, timeList, int(update['time'])))
			
						# SQLite version
						#cursor.execute("""INSERT OR REPLACE INTO bgpPrefixUpdates (prefix, asPath, timeList, updateTime) VALUES ('{0}', '{1}', '{2}', {3})""".format(prefix, asPath, timeList, int(update['time'])))
						conn.commit()
			
					else:
						(storedASPath, storedTimeList) = result
						if storedASPath == None:
							# This is the case where the prefix had been withdrawn.
							timeList = ' '.join([str(update['time'])] * asPathLength)
							cursor.execute("""UPDATE bgpPrefixUpdates SET asPath='{0}', timeList='{1}', updateTime={2} WHERE prefix='{3}'""".format(asPath, timeList, int(update['time']), prefix))
							conn.commit()
						else:
							if asPath == storedASPath:
								continue

							storedTimeListArray = storedTimeList.split(' ')
							storedASPathArray = storedASPath.split(' ')
				
							gvenASPathArray = asPath.split(' ')
							newTimeListArray = [str(update['time'])] * len(gvenASPathArray)
							for i in xrange(len(newTimeListArray)):
								if (len(storedTimeListArray) - i - 1 >= 0):
									newTimeListArray[len(newTimeListArray) - i - 1] = storedTimeListArray[len(storedTimeListArray) - i - 1]
								else:
									break
							newTimeList = ' '.join(newTimeListArray)
							# MySQL version
							# I don't get why this is an insert or update. We should be gaurenteed that this prefix exists.
							cursor.execute("""INSERT INTO bgpPrefixUpdates (prefix, asPath, timeList, updateTime) VALUES ('{0}', '{1}', '{2}', {3}) 
								ON DUPLICATE KEY UPDATE asPath='{4}', timeList='{5}', 
								updateTime={6}""".format(prefix, asPath, newTimeList, int(update['time']), asPath, newTimeList, int(update['time'])))
							#cursor.execute("""INSERT OR REPLACE INTO bgpPrefixUpdates (prefix, asPath, timeList, updateTime) VALUES ('{0}', '{1}', '{2}', {3})""".format(prefix, asPath, newTimeList, int(update['time'])))
							conn.commit()

				elif (update['type'] == 'W'):
					prefix = update['prefix']
					cursor.execute("""SELECT prefix, asPath, timeList, previousASPath, addedTime FROM bgpPrefixUpdates WHERE prefix='{0}'""".format(prefix))
					result = cursor.fetchone()
					if result == None:
						print "Route withdrawn from prefix that was not in DB. This is likely an error caused by improper seeding. Prefix: {0}".format(prefix)
					else:
						(_, storedASPath, storedTimeList, previousASPath, storedAddedTime) = result
						
						if storedASPath == None:
							print "Prefix withdrawn twice. Prefix: {0}".format(prefix)
						else:
							updateTime = int(update['time'])
							addedTime = updateTime - int(storedTimeList.split(' ')[0])
							if storedASPath == previousASPath:
								addedTime += storedAddedTime
							cursor.execute("""UPDATE bgpPrefixUpdates SET asPath=NULL, timeList=NULL, previousASPath='{0}', addedTime={1} WHERE prefix='{2}'""".format(storedASPath, addedTime, prefix))
							conn.commit()
			
				# Insert into the SQL server that we have processed an update at this time.
				# lastBGPUpdateProcessed is not neaded but it increases readability.
				lastBGPUpdateProcessed = update['time']
				updateLastBGPUpdateProcessed(lastBGPUpdateProcessed)

				# Print out a status update in terms of percentage of time processed.
				if printCounter == 1000:
					printCounter = 0
					cumulativePercentage = 100.0 * float(update['time'] - originalStartTime) / (endTime - originalStartTime)
					percentage = 100.0 * float(update['time'] - startTime) / (endTime - startTime)
					deltaT = time.time() - programStartTime
					secondsPerPercent = 0
					estimatedRemainingTime = 0
					if percentage == 0:
						secondsPerPercent = 0
						estimatedRemainingTime = 0
					else:
						secondsPerPercent = deltaT / percentage
						estimatedRemainingTime = secondsPerPercent * (100.0 - percentage)
					print "Certificate Time: {0}, Percentage: {1:3.3f}, Cumulative Percentage: {2:3.3f}, Estimated Time Remaining {3:9.0f}, Rate: {4}".format(update['time'], percentage, cumulativePercentage, estimatedRemainingTime, secondsPerPercent)
					sys.stdout.flush() # When running in the background output flushing is needed.
				printCounter += 1
		
		masterUpdateLoop()
		if not seeded:
			seeded = True
			cursor.execute("""UPDATE metadata SET stringVal='true' WHERE name = 'seeded'""")
			conn.commit()
			# To run the simulation with only seeding, add an exit here to prevent it from restarting the loop with new updates.
			print "!!!Done seeding DB.!!!"
			exit()
			print "!!!Beginning incremental update processing.!!!"
			initUpdates(True, lastBGPUpdateProcessed)
			masterUpdateLoop()
			