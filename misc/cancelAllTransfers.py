#!/usr/bin/env python

import os, sys, glob
import shutil
ceBase = "/afs/cern.ch/work/n/nraja/public/castor2echo/"
sys.path.append(ceBase)
from ftsJob import *
import fts3.rest.client.easy as fts3

sess = doTheSQLiteAndGetItsPointer()
rows = sess.query(ftsjob).filter().all()
finalStatuses = [ "FINISHED", "FAILED", "FINISHEDDIRTY", "CANCELLED" ]
toKillStatuses = [ "submitted", "SUBMITTED", "ACTIVE" ]
jobFiles = glob.glob(ceBase + "DOING/*.txt")

for jFile in jobFiles :
  # print jFile
  jobID = sess.query(ftsjob).filter(ftsjob.ftsFile==jFile.split("/")[-1]).all()
  if len(jobID) > 0:
    jobID = jobID[0]
  else:
    print jobID
    continue
  ftsJID = jobID.ftsID.strip()
  ftsServer = jobID.ftsServer.strip()
  # if not ("fts3.gridpp" in ftsServer): continue
  try:
    context = fts3.Context(ftsServer)
  except Exception, e:
    print "Exception creating FTS context ", e
    continue
  # print ftsServer
  print "Cancelling job : ", ftsJID, " file :", jFile.split("/")[-1], " status ", jobID.ftsStatus.strip()
  try:
    stat = fts3.cancel(context, ftsJID)
  except Exception, e:
    print "Exception in cancelling : ", e
    # sys.exit()
    continue

# for jobID in rows:
#   if jobID.ftsStatus.strip() in finalStatuses: continue
#   if jobID.ftsStatus.strip() in toKillStatuses:
#     fServer = jobID.ftsServer.strip()
#     ftsJID = jobID.ftsID.strip()
#     ftsFileName = jobID.ftsFile.strip()
#     context = fts3.Context(fServer)
#     print "Cancelling job : ", ftsJID, " file :", ftsFileName, " status ", jobID.ftsStatus.strip()
#     try:
#       stat = fts3.cancel(context, ftsJID)
#     except Exception, e:
#       print "Exception in cancelling : ", e
#       sys.exit()
#       continue
#   else:
#     print (jobID.ftsID.strip(), jobID.ftsStatus.strip(), jobID.ftsIter, jobID.ftsServer.strip(), jobID.ftsFile.strip())
