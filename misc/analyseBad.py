#!/usr/bin/env python
import os, sys, glob, shutil
ceBase = "/afs/cern.ch/work/n/nraja/public/castor2echo/"
sys.path.append(ceBase)
from ftsJob import *
import fts3.rest.client.easy as fts3

# Return a tuple containing the ID and current status of the job
def getStatusForJob(s, f):
  # print f
  jobID = s.query(ftsjob).filter(ftsjob.ftsFile==f).all()
  if len(jobID) == 1:
    jobID = jobID[0]
    return (jobID.ftsID.strip(), jobID.ftsStatus.strip(), jobID.ftsIter, jobID.ftsServer.strip())
  else: # Some error. No two jobs should have the same file name
    return ("-1", "-1", -1, "-1")

def lookAtFile(s, fN):
  tFN = fN.split("/")[-1]
  (ftsJID, stat, fIter, fServer) = getStatusForJob(s, tFN)
  if fServer == "-1":
    if tFN.startswith("M"):
      print "Unknown FTS job? Retry.", tFN
      shutil.move(fN, ceBase + "TODO/" + tFN)
    else:
      print "Unknown FTS job?", tFN
    return 0
  context = fts3.Context(fServer)
  try:
    jobStat = fts3.get_job_status(context, ftsJID, list_files=True)
    for fileInfo in jobStat['files']:
      reason = fileInfo["reason"]
      if "No such file or directory" in reason:
        print tFN, fServer, reason
        if "cern.ch" in fServer:
          print "Failed for CERN FTS server : retry"
          shutil.move(fN, ceBase + "TODO/" + tFN)
          return 0
      elif "TRANSFER CHECKSUM MISMATCH" in reason:
        print "Transfer checksum mismatch - retrying ", tFN
        shutil.move(fN, ceBase + "TODO/" + tFN)
        return 0
      elif "Probably stalled" in reason:
        print "Stalled transfer - retrying ", tFN
        shutil.move(fN, ceBase + "TODO/" + tFN)
        return 0
      elif "SOURCE SRM_GET_TURL error on the turl" in reason:
        print "srm failure : probably diskserver was down. Retry"
        shutil.move(fN, ceBase + "TODO/" + tFN)
        return 0
      else:
        print tFN, fServer, fileInfo["reason"]
        continue
    # print " .......... "
    # print fServer[:-1] + "9/fts3/ftsmon/#/job/" + ftsJID
    # print " .......... "
  except:
    print "Could not find any information for ", tFN, ". Try the transfer again."
    # shutil.move(fN, ceBase + "TODO/B" + tFN)
    return -1
  # print jobStat
  return 0

sess = doTheSQLiteAndGetItsPointer()
failedFiles = glob.glob(ceBase + "DONE/Bad/*.txt")
for ff in failedFiles:
  re = lookAtFile(sess, ff)
  # if re == 0:
  #   sys.exit()

