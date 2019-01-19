#!/usr/bin/env python

import os, sys, glob
import shutil
ceBase = "/afs/cern.ch/work/n/nraja/public/castor2echo/"
sys.path.append(ceBase)
from ftsJob import *
import fts3.rest.client.easy as fts3

sess = doTheSQLiteAndGetItsPointer()
rows = sess.query(ftsjob).filter().all()
x = 0
for jobID in rows:
  if jobID.ftsStatus.strip() == "FINISHED" : continue
  if jobID.ftsStatus.strip() == "FAILED" : continue
  if jobID.ftsStatus.strip() == "FINISHEDDIRTY" : continue
  if jobID.ftsStatus.strip() == "CANCELLED" : continue
  if jobID.ftsStatus.strip() == "submitted" or \
     jobID.ftsStatus.strip() == "SUBMITTED" : continue # or \
#     jobID.ftsStatus.strip() == "ACTIVE" :
  if jobID.ftsStatus.strip() == "ACTIVE" : # continue
    fServer = jobID.ftsServer.strip()
    ftsJID = jobID.ftsID.strip()
    ftsFileName = jobID.ftsFile.strip()
    context = fts3.Context(fServer)
    print "Cancelling job : ", ftsJID, " file :", ftsFileName
    try:
      stat = fts3.cancel(context, ftsJID)
    except:
      continue
  else:
    print (jobID.ftsID.strip(), jobID.ftsStatus.strip(), jobID.ftsIter, jobID.ftsServer.strip(), jobID.ftsFile.strip())
