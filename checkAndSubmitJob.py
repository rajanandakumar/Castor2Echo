#!/usr/bin/env python

# To be in DOING directory and run as a cron job, maybe once an hour or so
import os, sys, glob, shutil, random
import subprocess

ceBase = "/afs/cern.ch/work/n/nraja/public/castor2echo/"
sys.path.append(ceBase)
from ftsJob import *
import fts3.rest.client.easy as fts3
import fts3.rest.client as fsubmit

# Submit the FTS job to the FTS server and retrieve the FTS job ID
def submitTheFTSJob(ftsFile):
  # ftsServ = random.choice([ftsServ1, ftsServ2])
  rndValue = random.uniform(0.0,1.0)
  ftsServ = ftsServ1
  if rndValue < 0.5 : ftsServ = ftsServ2
  context = fts3.Context(ftsServ)
  filecontent = open(ceBase + "DOING/" + ftsFile).read().split("\n")
  transfers = []
  for ftra in filecontent:
    if len(ftra) < 10 : continue
    (sourceSURL, targetSURL) = ftra.split("  ")
    comm = "gfal-stat " + sourceSURL
    runComm = subprocess.Popen(comm, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
    theInfo = runComm.communicate()[1].strip()
    if theInfo.startswith("gfal-stat error: 2 (No such file or directory)"):
      bFTS = open(ceBase + "DONE/badFileList.txt", "a")
      bFTS.write(ftra + "\n")
      bFTS.close()
    else:
      transf = fts3.new_transfer(sourceSURL, targetSURL)
      transfers.append(transf)
  if len(transfers) > 0:
    # job = fts3.new_job(transfers=transfers, overwrite=True, verify_checksum=True, reuse=True, retry=5)
    job = fts3.new_job(transfers=transfers, overwrite=True, verify_checksum=True, reuse=False, retry=5) # requested by Andrea Manzi
    ftsJobID = fts3.submit(context, job)
    return ftsJobID, ftsServ
  else:
    return "-1", "-1"

c2eJobFiles = glob.glob(ceBase + "TODO/*.txt")
if len(c2eJobFiles) < 1 :
  print "No files to submit"
  sys.exit()
sess = doTheSQLiteAndGetItsPointer()

kount = 0
for theFile in c2eJobFiles:
  shutil.move(theFile, ceBase + "DOING/")
  theFile = theFile.split("/")[-1]
  ftsJobID, ftsServ = submitTheFTSJob(theFile)
  print "Submitted file : ", theFile, " with fts ID : ", ftsJobID, " to server ", ftsServ
  if ftsJobID == "-1": #continue
    if os.stat(theFile).st_size == 0:
      os.remove(theFile)
    else:
      print "Moving file", theFile, " to DONE/MissingInCastor"
      shutil.move(ceBase + "DOING/" + theFile, ceBase + "DONE/MissingInCastor/")
    continue
  #Now I have a pair - write them to the SQLite DB.
  m = sess.query(ftsjob).filter(ftsjob.ftsFile==theFile).all()
  if m:
    m = m[0]
    m.ftsID = ftsJobID
  else:
    newJob = ftsjob(ftsFile=theFile, ftsID=ftsJobID, ftsStatus="submitted", ftsIter=0, ftsServer=ftsServ)
    sess.add(newJob)
  sess.commit()
  kount = kount + 1
  # Submit at most 5 FTS jobs in one iteration of this script
  if kount >= 5 :
    sys.exit()
