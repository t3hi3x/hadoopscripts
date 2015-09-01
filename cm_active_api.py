#/bash/python

import urllib, json

DEBUG = False

CM_HOST = "cm.company.com"
CM_PORT = 7180
CM_USERNAME = "admin"
CM_PASSWORD = "admin"

CLUSTER_NAME = "cluster"
HDFS_SERVICE_NAME = 'hdfs'

API_VERSION = 8 # CM 5.2.x

# First, get the active NN role ID

url = "http://%s:%s@%s:%s/api/v%s/clusters/%s/services/%s/roles" % (CM_USERNAME, CM_PASSWORD, CM_HOST, CM_PORT, API_VERSION, CLUSTER_NAME, HDFS_SERVICE_NAME)

if DEBUG:
    print url
result = urllib.urlopen(url)

r = json.loads(result.read())

if DEBUG:
    print r

roleName = ""
roleHost_cm = ""
roleHost = ""
nameService = ""


for n in r['items']:
    if "NAMENODE" in n['type'] and 'haStatus' in n:
        if n['haStatus'] == "ACTIVE":
            roleName = n['name']

result.close()

# Second, get the host ID for the active NN

if roleName != "":
    url = "http://%s:%s@%s:%s/api/v%s/clusters/%s/services/%s/roles/%s/" % (CM_USERNAME, CM_PASSWORD, CM_HOST, CM_PORT, API_VERSION, CLUSTER_NAME, HDFS_SERVICE_NAME, roleName)
    if DEBUG:
        print url
    result = urllib.urlopen(url)
    r = json.loads(result.read())
    if DEBUG:
        print r

    roleHost_cm = r['hostRef']['hostId']
    if DEBUG:
        print "Host ID: %s" % roleHost_cm
else:
    print "Did not get roleID. Quitting."
    quit()

result.close()

# Finally, get the hostename

if roleHost_cm != "":
    url = "http://%s:%s@%s:%s/api/v%s/hosts/%s" % (CM_USERNAME, CM_PASSWORD, CM_HOST, CM_PORT, API_VERSION, roleHost_cm)
    if DEBUG:
        print url
    result = urllib.urlopen(url)
    r = json.loads(result.read())
    if DEBUG:
        print r

    roleHost = r['hostname']

    print roleHost
else:
    print "Did not get hostname. Quitting."

result.close()
