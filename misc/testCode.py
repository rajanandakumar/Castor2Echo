#!/usr/bin/env python

import os, sys, glob, shutil
ceBase = "/afs/cern.ch/work/n/nraja/public/castor2echo/"
sys.path.append(ceBase)
from ftsJob import *
import fts3.rest.client.easy as fts3

ftsFile = "FTSList-31052018-173109.txt"
sess = doTheSQLiteAndGetItsPointer()
# jobID = sess.query(ftsjob).filter(ftsjob.ftsFile==ftsFile).all()[0]
# fServer = jobID.ftsServer.strip()
# fid = jobID.ftsID.strip()
# context = fts3.Context(fServer)
# # ftsStat = fts3.get_job_status(context, fid)
# jobStat = fts3.get_job_status(context, fid, list_files=True)
# usefulInfo = ("job_id", "file_state", "source_surl", "dest_surl", "log_file", "transfer_host")
# for fileInfo in jobStat['files']:
#   if fileInfo["file_state"] == "FINISHED": continue
#   print fileInfo
#   print ftsFile
#   for ui in usefulInfo:
#     if ui == "log_file":
#       log = "https://" + fileInfo["transfer_host"] + ":8449" + fileInfo["log_file"]
#       print "   ", log
#     else:
#       print "   ", fileInfo[ui]
#   print
#   # print fileInfo['file_state'], fileInfo['job_id']

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
  ftsStat = fts3.get_job_status(context, fid)
  return ftsStat["job_state"], fid, ftsStat, fServer

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

def retryFailedTransfer(fFiles, ftsFileName):
  newF = "Retry-" + ftsFileName
  print newF
  fFTS = open(ceBase + "TODO/" + newF, "w")
  for line in fFiles:
    if len(line) != 2:
      continue
    # TODO : for now assume everything is going to "lhcb:prod"
    fFTS.write(line[0] + "  " + line[1] + "\n")
  fFTS.close()


def recoverFINISHEDDIRTY(s, ftsFileName):
  (status, ftsJID, fStat, fServer) = getNewStatus(s, ftsFileName)
  # This set of files has transferred successfully. Move it to DONE directory
  print "Finished dirty for ", ftsFileName, " with ftsID", ftsJID, " status", status, "."
  
  context = fts3.Context(fServer)
  jobStat = fts3.get_job_status(context, ftsJID, list_files=True)
  failedFiles = []
  for fileInfo in jobStat['files']:
    if fileInfo["file_state"] == "FINISHED": continue
    failedFiles.append((fileInfo["source_surl"], fileInfo["dest_surl"]))
  # print failedFiles
  for fF in failedFiles:
    print fF

  cleanUpTransfer(failedFiles, ftsFileName)
  retryFailedTransfer(failedFiles, ftsFileName)

    # print ftsFileName, ftsJID, fileInfo["file_state"]
    # print "    ", fileInfo["source_surl"]
    # print "    ", fileInfo["dest_surl"]
    # try:
    #   print "     https://" + fileInfo["transfer_host"] + ":8449" + fileInfo["log_file"]
    # except:
    #   print "     No log file ... transfer host : ", fileInfo["transfer_host"]

recoverFINISHEDDIRTY(sess, ftsFile)
