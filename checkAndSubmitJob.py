#!/usr/bin/python

# To be in DOING directory and run as a cron job, maybe once an hour or so
import os, sys, glob, shutil

ceBase = "/afs/cern.ch/work/n/nraja/public/castor2echo/"
sys.path.append(ceBase)
from ftsJob import *
import fts3.rest.client.easy as fts3
import fts3.rest.client as fsubmit

# First copy the earliest file over if there is one or more new files to be submitted as an FTS job
def copyFTSFileJob():
  c2eJobFiles = glob.glob(ceBase + "TODO/*.txt")
  c2eJobFiles.sort()
  fileSource = c2eJobFiles[0]
  shutil.move(fileSource, ceBase + "DOING/")
  return fileSource

# Submit the FTS job to the FTS server and retrieve the FTS job ID
def submitTheFTSJob(ftsFile):
  context = fts3.Context(ftsServ)
  filecontent = open(ceBase + "DOING/" + ftsFile).read().split("\n")
  transfers = []
  for ftra in filecontent:
    if len(ftra) < 10 : continue
    (sourceSURL, targetSURL) = ftra.split("  ")
    transf = fts3.new_transfer(sourceSURL, targetSURL)
    transfers.append(transf)
  job = fts3.new_job(transfers=transfers, overwrite=True, verify_checksum=True, reuse=True)
  ftsJobID = fts3.submit(context, job)
  return ftsJobID

theFile = copyFTSFileJob().split("/")[-1]
ftsJobID = submitTheFTSJob(theFile)
print "Submitted file : ", theFile, " with fts ID : ", ftsJobID

#Now I have a pair - write them to the SQLite DB.
sess = doTheSQLiteAndGetItsPointer()
newJob = ftsjob(ftsFile=theFile, ftsID=ftsJobID, ftsStatus="submitted", ftsIter=0)
sess.add(newJob)
sess.commit()
