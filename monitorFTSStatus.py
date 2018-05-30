#!/usr/bin/python

import os, sys, glob
import shutil
ceBase = "/afs/cern.ch/work/n/nraja/public/castor2echo/"
sys.path.append(ceBase)
from ftsJob import *
import fts3.rest.client.easy as fts3

# Return a tuple containing the ID and current status of the job
def getStatusForJob(s, f):
  jobID = s.query(ftsjob).filter(ftsjob.ftsFile==f).all()[0] # Assume only one row is returned
  return (jobID.ftsID.strip(), jobID.ftsStatus.strip(), jobID.ftsIter)

# Update the status for the job, and increment the iterations the job has been monitored
def updateFTSStatusForJob(s, f, stat):
  m = s.query(ftsjob).filter(ftsjob.ftsFile==f).all()[0]
  m.ftsStatus = stat
  uID = m.ftsID.strip()
  uIter = m.ftsIter + 1
  m.ftsIter = uIter
  s.commit()
  return (uIter, uID)

# Get the new status of the job from FTS
def getNewStatus(s, f, fid=""):
  if len(fid) < 3 :
    (fid, fstat, fIter) = getStatusForJob(s, f)
  context = fts3.Context(ftsServ)
  ftsStat = fts3.get_job_status(context, fid)
  # print ftsStat
  return ftsStat["job_state"]

sess = doTheSQLiteAndGetItsPointer()
jobFiles = glob.glob(ceBase + "DOING/*.txt")
for jFile in jobFiles :
  ftsFileName = jFile.split("/")[-1]
  status = getNewStatus(sess, ftsFileName)
  # This set of files has transferred successfully. Move it to DONE directory
  (nIter, ftsJID) = updateFTSStatusForJob(sess, ftsFileName, status)
  if status == "FINISHED":
    shutil.move(ceBase + "DOING/" + ftsFileName, ceBase + "DONE/Okay/")
  else :
    if nIter > 5:
      print "Trouble with files in job", ftsFileName, " with ftsID", ftsJID, " status", status
      # For now leave the files in the DOING directory
    else:
      print "Ongoing transfer for ", ftsFileName, " with ftsID", ftsJID, " status", status
