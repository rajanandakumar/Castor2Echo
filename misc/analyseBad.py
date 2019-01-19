#!/usr/bin/env python
import os, sys, glob, shutil
ceBase = "/afs/cern.ch/work/n/nraja/public/castor2echo/"
sys.path.append(ceBase)
from ftsJob import *
import fts3.rest.client.easy as fts3

# Return a tuple containing the ID and current status of the job
def getStatusForJob(s, f):
  # print f
  try:
    jobID = s.query(ftsjob).filter(ftsjob.ftsFile==f).all()
  except:
    jobID = -1
  if len(jobID) == 1:
    jobID = jobID[0]
    return (jobID.ftsID.strip(), jobID.ftsStatus.strip(), jobID.ftsIter, jobID.ftsServer.strip())
  else: # Some error. No two jobs should have the same file name
    return ("-1", "-1", -1, "-1")

def lookAtFile(s, fN):
  tFN = fN.split("/")[-1]
  (ftsJID, stat, fIter, fServer) = getStatusForJob(s, tFN)
  if fServer == "-1":
    print "Unknown FTS job? Retry.", tFN
    shutil.move(fN, ceBase + "TODO/" + tFN)
    # if tFN.startswith("M"):
    #  print "Unknown FTS job? Retry.", tFN
    #  shutil.move(fN, ceBase + "TODO/" + tFN)
    # else:
    #  print "Unknown FTS job?", tFN
    return 0
  context = fts3.Context(fServer)
  try:
    jobStat = fts3.get_job_status(context, ftsJID, list_files=True)
    for fileInfo in jobStat['files']:
      # print(tfn,)
      reason = fileInfo["reason"]
      if "No such file or directory" in reason:
        # print tFN, fServer, reason
#         if "cern.ch" in fServer:
#           print "Failed for CERN FTS server : retry"
# #          shutil.move(fN, ceBase + "TODO/" + tFN)
#           return 0
        if "SOURCE" in reason:
          print "Missing source : ", fileInfo["source_surl"].split("SFN=")[1], fServer[:-1] + "9/fts3/ftsmon/#/job/" + ftsJID

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
      elif "Communication error on send" in reason:
        print "srm failure : Known (old) problem with RAL FTS system. Retry"
        shutil.move(fN, ceBase + "TODO/" + tFN)
        return 0
      elif "Transfer canceled because the gsiftp performance marker timeout" in reason:
        print "Recoverable error : 6 minute timeout exceeded. Retry"
        shutil.move(fN, ceBase + "TODO/" + tFN)
        return 0
      elif "bad data was encountered" in reason:
        print "Recoverable error : Command failed. : bad data was encountered. Retry"
        shutil.move(fN, ceBase + "TODO/" + tFN)
        return 0
      elif "Command failed : error: commands denied" in reason:
        print "Recoverable error : Command failed : error: commands denied. Retry"
        shutil.move(fN, ceBase + "TODO/" + tFN)
        return 0
      else:
        print tFN, fServer[:-1] + "9/fts3/ftsmon/#/job/" + ftsJID, fileInfo["reason"]
        print fileInfo["source_surl"].split("SFN=")[1]
        continue
    # print " .......... "
    # print fServer[:-1] + "9/fts3/ftsmon/#/job/" + ftsJID
    # print " .......... "
  except:
    print "Could not find any information for ", tFN, ". Try the transfer again."
    shutil.move(fN, ceBase + "TODO/B" + tFN)
    return -1
  # print jobStat
  return 0

sess = doTheSQLiteAndGetItsPointer()
failedFiles = glob.glob(ceBase + "DONE/Bad/*.txt")
for ff in failedFiles:
  re = lookAtFile(sess, ff)
  # if re == 0:
  #   sys.exit()

