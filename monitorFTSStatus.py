#!/usr/bin/env python

import os, sys, glob
import shutil
ceBase = "/afs/cern.ch/work/n/nraja/public/castor2echo/"
sys.path.append(ceBase)
from ftsJob import *
import fts3.rest.client.easy as fts3

# Return a tuple containing the ID and current status of the job
def getStatusForJob(s, f):
  # print f
  jobID = []
  try: # Try thrice
    jobID = s.query(ftsjob).filter(ftsjob.ftsFile==f).all()
  except:
    try:
      jobeID = s.query(ftsjob).filter(ftsjob.ftsFile==f).all()
    except:
      try:
        jobeID = s.query(ftsjob).filter(ftsjob.ftsFile==f).all()
      except:
        return ("-1", "-1", -1, "-1")
  if len(jobID) == 1:
    jobID = jobID[0]
    return (jobID.ftsID.strip(), jobID.ftsStatus.strip(), jobID.ftsIter, jobID.ftsServer.strip())
  else: # Some error. No two jobs should have the same file name
    return ("-1", "-1", -1, "-1")

# Update the status for the job, and increment the iterations the job has been monitored
def updateFTSStatusForJob(s, f, stat):
  m = []
  try: # Try thrice
    m = s.query(ftsjob).filter(ftsjob.ftsFile==f).all()
  except:
    try:
      m = s.query(ftsjob).filter(ftsjob.ftsFile==f).all()
    except:
      try:
        m = s.query(ftsjob).filter(ftsjob.ftsFile==f).all()
      except:
        return (-1, "-1")
  if len(m) == 1:
    m = m[0]
  else: # Some error. No two jobs should have the same file name
    return (-1, "-1")
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
    return "Unknown-notsubmitted", -1, 0, "-1"
  context = fts3.Context(fServer)
  try:
    ftsStat = fts3.get_job_status(context, fid)
    return ftsStat["job_state"], fid, ftsStat, fServer
  except:
    print "File ", f, "unknown to FTS?"
    return "Unknow", -1, 0, "-1"

def cleanUpTransfer(fFiles, ftsFileName):
  inFile = ceBase + "DOING/" + ftsFileName
  tmpFile1 = ceBase + "DONE/temp1.txt"
  tmpFile2 = ceBase + "DONE/temp2.txt"
  outFile = ceBase + "DONE/Okay/" + ftsFileName
  shutil.copy(inFile, tmpFile1)
  for fF in fFiles:
    fn = fF[0].split("/")[-1]
    command = "grep -v " + fn + " " +  tmpFile1 + " > " + tmpFile2 + " ; mv " + tmpFile2 + " " + tmpFile1
    # print command
    os.system(command)
  # I have a clean temp.txt now. Move it to DONE/Okay and remove the Dirty file
  shutil.move(tmpFile1, outFile)
  os.remove(inFile)

def writeTransfer(fFiles, fDir, fPrefix, ftsFileName):
  if len(fFiles) == 0:
    return
  newF = fPrefix + ftsFileName
  # print newF
  fFTS = open(ceBase + fDir + newF, "w")
  for line in fFiles:
    if len(line) != 2:
      continue
    # TODO : for now assume everything is going to "lhcb:prod"
    fFTS.write(line[0] + "  " + line[1] + "\n")
  fFTS.close()

def recoverFINISHEDDIRTY(s, ftsFileName):
  (status, ftsJID, fStat, fServer) = getNewStatus(s, ftsFileName)
  # This set of files has transferred successfully. Move it to DONE directory
  # print "Finished dirty for ", ftsFileName, " with ftsID", ftsJID, " status", status, "."  
  context = fts3.Context(fServer)
  jobStat = fts3.get_job_status(context, ftsJID, list_files=True)
  failedFiles = []
  missFiles = []
  for fileInfo in jobStat['files']:
    if fileInfo["file_state"] == "FINISHED": continue
    reason = fileInfo["reason"]
    if "Probably stalled" in reason:
      failedFiles.append((fileInfo["source_surl"], fileInfo["dest_surl"]))
    elif "globus_ftp_control_local_pasv failed" in reason:
      failedFiles.append((fileInfo["source_surl"], fileInfo["dest_surl"]))
    elif "500 No such file or directory" in reason:
      print fServer, fileInfo["source_surl"], reason
      print fServer[:-1] + "9/fts3/ftsmon/#/job/" + ftsJID
      missFiles.append((fileInfo["source_surl"], fileInfo["dest_surl"]))
    else:
      print ftsFileName, fileInfo["source_surl"], fileInfo["reason"]
      failedFiles.append((fileInfo["source_surl"], fileInfo["dest_surl"]))
  # print failedFiles
  cleanUpTransfer(failedFiles, ftsFileName)
  writeTransfer(failedFiles, "TODO/", "D", ftsFileName)
  writeTransfer(missFiles, "DONE/Bad/", "M", ftsFileName)
  # writeTransfer(missFiles, "TODO/", "M", ftsFileName)


sess = doTheSQLiteAndGetItsPointer()
jobFiles = glob.glob(ceBase + "DOING/*.txt")
jobsChecked = []
nFinished = 0
nDirty = 0
nOther = 0
nFailed = 0
nActive = 0
nSubmitted = 0
for jFile in jobFiles :
  ftsFileName = jFile.split("/")[-1]
  (status, ftsJID, fStat, fServer) = getNewStatus(sess, ftsFileName)
  # This set of files has transferred successfully. Move it to DONE directory
  (nIter, ftsJID) = updateFTSStatusForJob(sess, ftsFileName, status)
  if fServer != "-1":
    jobsChecked.append((ftsFileName, status, fServer, ftsJID))
  else :
    print "File ", ftsFileName, "not submitted to FTS? Moving back to TODO"
    print "Status : ", status
    print "FTSID : ", ftsJID
    print "FTS job latest status:", fStat
    print "FTS server:", fServer
    print "NIterations:", nIter
    shutil.move(ceBase + "DOING/" + ftsFileName, ceBase + "TODO/u" +ftsFileName)
    continue
  if status == "FINISHED":
    if nFinished < 10:
      print "All done for ", ftsFileName, " with ftsID", ftsJID, " status", status, ". Moving to DONE/Okay."
    shutil.move(ceBase + "DOING/" + ftsFileName, ceBase + "DONE/Okay/")
    nFinished = nFinished + 1
  elif status == "FINISHEDDIRTY":
    if nDirty < 10:
      print "Finished dirty for ", ftsFileName, " with ftsID", ftsJID, " status", status, ". Retrying failed files."
    recoverFINISHEDDIRTY(sess, ftsFileName)
    nDirty = nDirty + 1
  elif status == "ACTIVE":
    if nActive < 10:
      print "Ongoing transfer for ", ftsFileName, " : ", fServer[:-1] + "9/fts3/ftsmon/#/job/" + ftsJID
    nActive = nActive + 1
  elif status == "FAILED":
    print "Failed transfer : ",
    if ftsFileName.startswith("R-") or ftsFileName.startswith("uuu") or ftsFileName.startswith("DD"):
      # Likely pathologically bad. To be checked
      if nFailed < 10:
        print "Bad failure for ", ftsFileName, " with ftsID", ftsJID, " status", status, ". Moving to DONE/Bad."
      shutil.move(ceBase + "DOING/" + ftsFileName, ceBase + "DONE/Bad/")
    else:
      # Try again
      if nFailed < 10:
        print " Not so bad failure for ", ftsFileName, " with ftsID", ftsJID, ". Try again."
      shutil.move(ceBase + "DOING/" + ftsFileName, ceBase + "TODO/u" + ftsFileName)
    nFailed = nFailed + 1
  elif status == "SUBMITTED":
    if (nSubmitted < 10) or ("cern" in fServer):
      print "Waiting to be picked up by FTS :", ftsFileName, " : ", fServer[:-1] + "9/fts3/ftsmon/#/job/" + ftsJID
    nSubmitted = nSubmitted + 1
  else :
    # if nIter > 5:
    #   print "Trouble with files in job", ftsFileName, " with ftsID", ftsJID, " status", status, " on server", fServer
    #   # For now leave the files in the DOING directory
    # else:
    if nOther < 10:
      print "Ongoing transfer for ", ftsFileName, " with ftsID", ftsJID, " status", status, " on server", fServer
    nOther = nOther + 1

print
print "Summary of Transfers"
print "ACTIVE : ", nActive
print "FINISHED : ", nFinished
print "FINISHEDDIRTY : ", nDirty
print "FAILED : ", nFailed
print "SUBMITTED : ", nSubmitted
print "Others : ", nOther
print

# jobsChecked = sorted(jobsChecked, key=lambda tup:tup[1])
# # print jobsChecked
# for j in jobsChecked:
#   serv = j[2][:-4]
#   print j
#   print j[0], " ", j[1], " ", serv + "8449/fts3/ftsmon/#/job/" + j[3]
