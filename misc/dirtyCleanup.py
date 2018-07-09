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

# Get the new status of the job from FTS
def getNewStatus(s, f, fid=""):
  if len(fid) < 3 :
    (fid, fstat, fIter, fServer) = getStatusForJob(s, f)
  if fid == "-1":
    print "File ", f, "not submitted to FTS?"
    return "Unknown-notsubmitted", -1, 0, "-1"
  context = fts3.Context(fServer)
  try:
    ftsStat = fts3.get_job_status(context, fid)
    return ftsStat["job_state"], fid, ftsStat, fServer
  except:
    return "Unknown", -1, 0, "-1"

def cleanUpTransfer(fFiles, ftsFileName):
  inFile = ceBase + "DONE/Dirty/" + ftsFileName
  tmpFile1 = ceBase + "DONE/temp1.txt"
  tmpFile2 = ceBase + "DONE/temp2.txt"
  outFile = ceBase + "DONE/Okay/" + ftsFileName
  shutil.copy(inFile, tmpFile1)
  for fF in fFiles:
    fn = fF[0].split("/")[-1]
    command = "grep -v " + fn + " " +  tmpFile1 + " > " + tmpFile2 + " ; mv " + tmpFile2 + " " + tmpFile1
    print command
    os.system(command)
  # I have a clean temp.txt now. Move it to DONE/Okay and remove the Dirty file
  shutil.move(tmpFile1, outFile)
  os.remove(inFile)

def writeTheTransfers(tFil, fNam):
  fFTS = open(ceBase + "TODO/" + fNam, "w")
  for line in tFil:
    if len(line) != 2:
      continue
    fFTS.write(line[0] + "  " + line[1] + "\n")
  fFTS.close()


def retryFailedTransfer(fFiles, ftsFileName):
  newF = "R-" + ftsFileName
  print "Retrying ---", newF
  writeTheTransfers(fFiles, newF)

def recoverFINISHEDDIRTY(s, ftsFileName):
  (status, ftsJID, fStat, fServer) = getNewStatus(s, ftsFileName)
  if status == "Unknown":
    # Try again ...
    shutil.move(ceBase + "DONE/Dirty/" + ftsFileName, ceBase + "TODO/" + ftsFileName)
    return
  # This set of files has transferred successfully. Move it to DONE directory
  print "Finished dirty for ", ftsFileName, " with ftsID", ftsJID, " status", status, "."
  if ftsJID == -1:
    print "Probably in old sqlite dB. Could not check - retry"
    shutil.move(ceBase + "DONE/Dirty/" + ftsFileName, ceBase + "TODO/" + ftsFileName)
    return
  context = fts3.Context(fServer)
  jobStat = fts3.get_job_status(context, ftsJID, list_files=True)
  failedFiles = []
  for fileInfo in jobStat['files']:
    if fileInfo["file_state"] == "FINISHED": continue
    failedFiles.append((fileInfo["source_surl"], fileInfo["dest_surl"]))
  # print failedFiles
  if len(failedFiles) < 1: return
  cleanUpTransfer(failedFiles, ftsFileName)
  retryFailedTransfer(failedFiles, ftsFileName)

sess = doTheSQLiteAndGetItsPointer()
failedFiles = glob.glob(ceBase + "DONE/Dirty/*.txt")
for fF in failedFiles:
  fName = fF.split("/")[-1]
  print "Processing ... ", fName
  recoverFINISHEDDIRTY(sess, fName)
