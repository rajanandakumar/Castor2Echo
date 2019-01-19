#!/usr/bin/env python

# from DIRAC.Core.Base.Script import parseCommandLine
# parseCommandLine()

import os, sys
import subprocess
import time

ceBase = "/afs/cern.ch/work/n/nraja/public/castor2echo/"
nLFNs = 100
# lfnListFile = "/afs/cern.ch/work/n/nraja/public/castor2echo/TODO/ListOfLFNs/RAL-USER.list"
# lfnListFile = "/afs/cern.ch/work/n/nraja/public/castor2echo/TODO/ListOfLFNs/RAL-Buffer.list"
# lfnListFile = "/afs/cern.ch/work/n/nraja/public/castor2echo/TODO/ListOfLFNs/reSync-DST-MCDST-19Dec2018.list"
# lfnListFile = "/afs/cern.ch/work/n/nraja/public/castor2echo/TODO/ListOfLFNs/reSync-DST-15Jan2019.list"
# lfnListFile = "/afs/cern.ch/work/n/nraja/public/castor2echo/TODO/ListOfLFNs/reSync-MC-DST-16Jan2019.list"
lfnListFile = "/afs/cern.ch/work/n/nraja/public/castor2echo/TODO/ListOfLFNs/reSync-USER-17Jan2019.list"

# To be in TODO directory, to be run as and when I feel it - or maybe as a cron job

def writeFileSet(lFiles, pref):
  # lFiles is a list of LFNs which I will then write pairwise into a single file for
  # submission as an fts job
  castorPrefix = "srm://srm-lhcb.gridpp.rl.ac.uk:8443/srm/managerv2?SFN=/castor/ads.rl.ac.uk/prod"
  echoPrefix = "gsiftp://gridftp.echo.stfc.ac.uk/lhcb:" + pref
  timestr = time.strftime("%d%m%Y-%H%M%S")
  ftsFileList = "FTSList-" + timestr + ".txt"
  # print ftsFileList
  fFTS = open(ceBase + "TODO/" + ftsFileList, "w")
  nWritten = 0
  for line in lFiles:
    if len(line) < 10:
      continue
    # TODO : for now assume everything is going to "lhcb:prod"
    # comm = "gfal-stat " + castorPrefix + line
    # runComm = subprocess.Popen(comm, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
    # theInfo = runComm.communicate()[1].strip()
    # if theInfo.startswith("gfal-stat error: 2 (No such file or directory)"):
    #   bFTS = open(ceBase + "DONE/badFileList.txt", "a")
    #   bFTS.write(castorPrefix + line + "  " + echoPrefix + line + "\n")
    #   bFTS.close()
    # else:
    #   nWritten = nWritten + 1
    fFTS.write(castorPrefix + line + "  " + echoPrefix + line + "\n")
  fFTS.close()
  # if nWritten == 0: # No good files in this list!
  #   os.remove(ceBase + "TODO/" + ftsFileList)

# # TODO : For now hard code the file list. Later we will get this list from the DFC
# # We will also have to make sure somehow that the file is not already handled previously
# testList = [
# "/lhcb/LHCb/Collision10/FULL.DST/00037160/0000/00037160_00005496_1.full.dst",
# "/lhcb/LHCb/Collision10/FULL.DST/00037160/0000/00037160_00005495_1.full.dst"
# ]

# To make sure that the test fails - do not want to touch the production instance even by mistake
# testList = [ "test1.txt", "test2.castor", "test3.dcache", "test4.root", "test5.xroot", "test6.whatever"]

# Get the first nLFNs lines first
if os.stat(lfnListFile).st_size == 0 :
  sys.exit()
com1 = "head -n %s %s" %(str(nLFNs), lfnListFile)
testList = os.popen(com1).read().strip().split("\n")
# Remove the first nLFNs lines from the file
com2 = "sed -i -e '1,%sd' %s" %(str(nLFNs), lfnListFile)
os.system(com2)

# If the file name does not contain "USER" or "BUFFER", assume it is prod
prefix = "prod"
if "USER" in lfnListFile : prefix = "user"
elif "BUFFER" in lfnListFile : prefix = "buffer"
writeFileSet(testList, prefix)
