#!/usr/bin/python

import os, sys, glob
import shutil
ceBase = "/home/ppd/nraja/castorToEcho/"
sys.path.append(ceBase)
from ftsJob import *

# Return a tuple containing the ID and current status of the job
def getFTSStatusForJob(s, f):
  jobID = s.query(ftsjob).filter(ftsjob.ftsFile==f).all()[0] # Assume only one row is returned
  return (jobID.ftsID.strip(), jobID.ftsStatus.strip(), jobID.ftsIter)

# Update the status for the job, and increment the iterations the job has been monitored
def updateFTSStatusForJob(s, f, stat):
  m = s.query(ftsjob).filter(ftsjob.ftsFile==f).all()[0]
  m.ftsStatus = stat
  m.ftsIter = m.ftsIter + 1
  s.commit()

# Get the new status of the job from FTS
def getNewStatus(s, f, fid=""):
  if len(fid) < 3 :
    (fid, fstat, fIter) = getFTSStatusForJob(s, f)
  comm = "fts-transfer-status"
  argument =  "-s https://lcgfts3.gridpp.rl.ac.uk:8446 "
  command = comm + " " + argument + fid
  ftsStat = os.popen(command).read().strip()
  return ftsStat

sess = doTheSQLiteAndGetItsPointer()
jobFiles = glob.glob(ceBase + "DOING/*.txt")
for jFile in jobFiles :
  ftsFileName = jFile.split("/")[-1]
  status = getNewStatus(sess, ftsFileName)
  # This set of files has transferred successfully. Move it to DONE directory
  print ftsFileName, status
  (a,b,c) = getFTSStatusForJob(sess, ftsFileName)
  print a,b,c
  updateFTSStatusForJob(sess, ftsFileName, status)
  (a,b,c) = getFTSStatusForJob(sess, ftsFileName)
  print a,b,c
  if status == "Finished":
    shutil.move(ftsFileName, ceBase + "DONE/Okay/")
  