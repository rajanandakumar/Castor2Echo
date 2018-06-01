#!/usr/bin/python

import os, sys, glob
import shutil
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

# Update the status for the job, and increment the iterations the job has been monitored
def updateFTSStatusForJob(s, f, stat):
  m = s.query(ftsjob).filter(ftsjob.ftsFile==f).all()
  if len(m) == 1:
    m = m[0]
  else: # Some error. No two jobs should have the same file name
    return (-1, -1)
  m.ftsStatus = stat
  uID = m.ftsID.strip()
  uIter = m.ftsIter + 1
  m.ftsIter = uIter
  s.commit()
  return (uIter, uID)

# Get the new status of the job from FTS
def getNewStatus(s, f, fid=""):
  if len(fid) < 3 :
    (fid, fstat, fIter, fServer) = getStatusForJob(s, f)
  if fid == "-1":
    print "File ", f, "not submitted to FTS?"
    return "Unknown-notsubmitted", 0, "-1"
  context = fts3.Context(fServer)
  ftsStat = fts3.get_job_status(context, fid)
  return ftsStat["job_state"], ftsStat, fServer

sess = doTheSQLiteAndGetItsPointer()
jobFiles = glob.glob(ceBase + "DOING/*.txt")
jobsChecked = []
nFinished = 0
nDirty = 0
nOther = 0
for jFile in jobFiles :
  ftsFileName = jFile.split("/")[-1]
  (status, fStat, fServer) = getNewStatus(sess, ftsFileName)
  # This set of files has transferred successfully. Move it to DONE directory
  (nIter, ftsJID) = updateFTSStatusForJob(sess, ftsFileName, status)
  if fServer != "-1":
    jobsChecked.append((ftsFileName, status, fServer, ftsJID))
  if status == "FINISHED":
    print "All done for ", ftsFileName, " with ftsID", ftsJID, " status", status, ". Moving to DONE/Okay."
    shutil.move(ceBase + "DOING/" + ftsFileName, ceBase + "DONE/Okay/")
    nFinished = nFinished + 1
  elif status == "FINISHEDDIRTY":
    # print "Trouble with files in job", ftsFileName, " with ftsID", ftsJID, " status", status, " on server", fServer
    # print fStat
    context = fts3.Context(fServer)
    jobStat = fts3.get_job_status(context, ftsJID, list_files=True)
    usefulInfo = ("job_id", "file_state", "source_surl", "dest_surl", "log_file", "transfer_host")
    for fileInfo in jobStat['files']:
      if fileInfo["file_state"] == "FINISHED": continue
      print ftsFileName, ftsJID, fileInfo["file_state"]
      print "    ", fileInfo["source_surl"]
      print "    ", fileInfo["dest_surl"]
      try:
        print "     https://" + fileInfo["transfer_host"] + ":8449" + fileInfo["log_file"]
      except:
        print "     No log file ... transfer host : ", fileInfo["transfer_host"]
    nDirty = nDirty + 1
  else :
    # if nIter > 5:
    #   print "Trouble with files in job", ftsFileName, " with ftsID", ftsJID, " status", status, " on server", fServer
    #   # For now leave the files in the DOING directory
    # else:
    #   print "Ongoing transfer for ", ftsFileName, " with ftsID", ftsJID, " status", status, " on server", fServer
    nOther = nOther + 1

print
print "Summary of Transfers"
print "FINISHED : ", nFinished
print "FINISHEDDIRTY : ", nDirty
print "Others : ", nOther
print

jobsChecked = sorted(jobsChecked, key=lambda tup:tup[1])
# print jobsChecked
for j in jobsChecked:
  serv = j[2][:-4]
  print j[0], " ", j[1], " ", serv + "8449/fts3/ftsmon/#/job/" + j[3]
