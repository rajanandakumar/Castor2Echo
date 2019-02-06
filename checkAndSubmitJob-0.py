#!/usr/bin/env python

# To be in DOING directory and run as a cron job, maybe once an hour or so
import os, sys, glob, shutil, random
import subprocess, threading

ceBase = "/afs/cern.ch/work/n/nraja/public/castor2echo/"
sys.path.append(ceBase)
from ftsJob import *
import fts3.rest.client.easy as fts3
import fts3.rest.client as fsubmit

listOfPairs = []
okayFiles = []
checkStatus = False

def checkOneFile(num):
  # Keep the function running as long as there is some file to check
  while len(listOfPairs) > 0:
    # Pop out one file in the list to be checked
    onePair = listOfPairs.pop()
    if len(onePair) < 10: continue
    (sourceSURL, targetSURL) = onePair.split("  ")
    # Do the checking of the file
    comm = "gfal-stat " + sourceSURL
    runComm = subprocess.Popen(comm, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
    theInfo = runComm.communicate()[1].strip()
    if theInfo.startswith("gfal-stat error: 2 (No such file or directory)"):
      bFTS = open(ceBase + "DONE/badFileList.txt", "a")
      bFTS.write(onePair + "\n")
      bFTS.close()
    else:
      okayFiles.append((sourceSURL, targetSURL))
  return

def checkStatusOnCastor():
  # The threaded stuff here. Needs to be done carefully.
  # The function "checkOneFile" should independently do the needed processing.
  okayFiles[:] = []
  nThr = 10
  t = [0]*nThr
  for i in range(nThr):
    t[i] = threading.Thread(target=checkOneFile, args=(i,))
    t[i].start() # Run the thread
  for i in range(nThr):
    t[i].join() # Wait till the thread finishes before returning

# Submit the FTS job to the FTS server and retrieve the FTS job ID
def submitTheFTSJob(ftsFile):
  ### First way : Random choice of two servers
  # ftsServ = random.choice([ftsServ1, ftsServ2])
  ### Second way : Weighted choice of two servers
  # rndValue = random.uniform(0.0,1.0)
  # ftsServ = ftsServ1
  # if rndValue < 0.7 : ftsServ = ftsServ2
  ### Third way : Random choice of three servers
  fList = [ftsServ1, ftsServ2, ftsServ3]
  ftsServ = random.choice(fList)
  #
  context = fts3.Context(ftsServ)
  # Open the file and stop the processing.
  listOfPairs[:] = open(ceBase + "DOING/" + ftsFile).read().split("\n")
  # listOfPairs[:] = open(ceBase + "TODO/" + ftsFile).read().split("\n")
  # All the threading bit is here to check in parallel whether the files we are looking are okay in castor
  # Once the function is done, the list "okayFiles" should be filled
  transfers = []
  if checkStatus:
    checkStatusOnCastor()
  else:
    okayFiles[:] = []
    for onePair in listOfPairs:
      if len(onePair)<10: continue
      (sourceSURL, targetSURL) = onePair.split("  ")
      okayFiles.append((sourceSURL, targetSURL))
  if len(okayFiles)>0:
    for oneSet in okayFiles:
      transf = fts3.new_transfer(oneSet[0], oneSet[1])
      transfers.append(transf)
    job = fts3.new_job(transfers=transfers, overwrite=True, verify_checksum=True, reuse=False, retry=5) # requested by Andrea Manzi
    ftsJobID = fts3.submit(context, job)
    return ftsJobID, ftsServ
  else: # None of the files in this lot were good!
    return "-1", "-1"
  # return "-1", "-1"

c2eJobFiles = glob.glob(ceBase + "TODO/*.txt")
if len(c2eJobFiles) < 1 :
  print "No files to submit"
  sys.exit()
sess = doTheSQLiteAndGetItsPointer()

kount = 0
# c2eJobFiles = [ '/afs/cern.ch/work/n/nraja/public/castor2echo/TODO/FTSList-21012019-144747.txt' ]
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
