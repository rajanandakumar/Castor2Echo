#!/usr/bin/python

# To be in DOING directory and run as a cron job, maybe once an hour or so
import os, sys, glob, shutil

ceBase = "/afs/cern.ch/work/n/nraja/public/castor2echo/"
sys.path.append(ceBase)
from ftsJob import *
import fts3.rest.client.easy as fts3

# First copy the earliest file over if there is one or more new files to be submitted as an FTS job
def copyFTSFileJob():
  c2eJobFiles = glob.glob(ceBase + "TODO/*.txt")
  c2eJobFiles.sort()
  print c2eJobFiles
  fileSource = c2eJobFiles[0]
  # shutil.move(fileSource, ceBase + "DOING/")
  return fileSource

# Submit the FTS job to the FTS server and retrieve the FTS job ID
def submitTheFTSJob(ftsFile):
  context = fts3.Context(ftsServ)
  trans = fts3.new_transfer(sourceSURL,
                            targetSURL,
                            checksum='ADLER32:%s' % ftsFile.checksum,
                            filesize=ftsFile.size,
                            metadata=getattr(ftsFile, 'fileID'),
                            activity=self.activity)
  transfers.append(trans)

  # comm = "fts-transfer-submit"
  # argument =  "-K -o -s https://lcgfts3.gridpp.rl.ac.uk:8446 --file"
  # command = comm + " " + argument + " " + ftsFile
  # print command
  # ftsJobID = os.popen(command).read()
  return ftsJobID

theFile = copyFTSFileJob().split("/")[-1]
ftsJobID = submitTheFTSJob(theFile)

#Now I have a pair - write them to the SQLite DB.
sess = doTheSQLiteAndGetItsPointer()
newJob = ftsjob(ftsFile=theFile, ftsID=ftsJobID, ftsStatus="submitted", ftsIter=0)
sess.add(newJob)
sess.commit()
