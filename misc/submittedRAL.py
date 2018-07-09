#!/usr/bin/env python
import os, sys, glob, shutil, random
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

# Submit the FTS job to the FTS server and retrieve the FTS job ID
def submitTheFTSJob(context, ftsFile):
  ftsServ = random.choice([ftsServ1, ftsServ2])
  with open(ceBase + "DOING/" + ftsFile) as fF:
    filecontent = fF.read().split("\n")
  # filecontent = open(ceBase + "DOING/" + ftsFile).read().split("\n")
  transfers = []
  for ftra in filecontent:
    if len(ftra) < 10 : continue
    (sourceSURL, targetSURL) = ftra.split("  ")
    transf = fts3.new_transfer(sourceSURL, targetSURL)
    transfers.append(transf)
  if len(transfers) > 0:
    # job = fts3.new_job(transfers=transfers, overwrite=True, verify_checksum=True, reuse=True, retry=5)
    job = fts3.new_job(transfers=transfers, overwrite=True, verify_checksum=True, reuse=False, retry=5) # requested by Andrea Manzi
    ftsJobID = fts3.submit(context, job)
    return ftsJobID, ftsServ
  else:
    return "-1", "-1"

sess = doTheSQLiteAndGetItsPointer()
# submittedFiles = glob.glob(ceBase + "DOING/*.txt")
submittedFiles = glob.glob(ceBase + "DOING/RALSpecific/*.txt")
# print getStatusForJob(sess, "R-uFTSList-02062018-230631.txt")
kount = 0
for ff in submittedFiles:
  tFN = ff.split("/")[-1]
  # tFN = "R-uFTSList-02062018-230631.txt"
  (fID, fStat, fIter, fServ) = getStatusForJob(sess, tFN)
  if fServ != "https://lcgfts3.gridpp.rl.ac.uk:8446":
    # continue
    sys.exit()
  context = fts3.Context(fServ)
  # We have jobs submitted  to the RAL FTS server only
  # First cancel the job
  # print "Cancelling job : ", fID, " file :", tFN
  # stat = fts3.cancel(context, fID)
  shutil.move(ceBase + "DOING/RALSpecific/" + tFN, ceBase + "DOING/" + tFN)
  ftsJobID, ftsServ = submitTheFTSJob(context, tFN)
  print "Submitted file : ", tFN, " with fts ID : ", ftsJobID, " to server ", ftsServ, " URL:", ftsServ[:-1] + "9/fts3/ftsmon/#/job/" + ftsJobID
  # shutil.move(ceBase + "DOING/" + tFN, ceBase + "DOING/RALSpecific/" + tFN)
  if ftsJobID == "-1": continue
  #Now I have a pair - write them to the SQLite DB.
  m = sess.query(ftsjob).filter(ftsjob.ftsFile==tFN).all()
  if m:
    m = m[0]
    m.ftsID = ftsJobID
  else:
    newJob = ftsjob(ftsFile=tFN, ftsID=ftsJobID, ftsStatus="submitted", ftsIter=0, ftsServer=ftsServ)
    sess.add(newJob)
  sess.commit()
  kount = kount + 1
  if kount > 10:
    sys.exit()
