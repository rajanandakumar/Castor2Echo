#!/usr/bin/python

# from DIRAC.Core.Base.Script import parseCommandLine
# parseCommandLine()

import os
import time

ceBase = "/afs/cern.ch/work/n/nraja/public/castor2echo/"
nLFNs = 10
lfnListFile = "/afs/cern.ch/work/n/nraja/public/castor2echo/TODO/ListOfLFNs/MC-DST_30.05.18_0.txt"

# To be in TODO directory, to be run as and when I feel it - or maybe as a cron job

def writeFileSet(lFiles):
  # lFiles is a list of LFNs which I will then write pairwise into a single file for
  # submission as an fts job
  castorPrefix = "srm://srm-lhcb.gridpp.rl.ac.uk:8443/srm/managerv2?SFN=/castor/ads.rl.ac.uk/prod"
  echoPrefix = "gsiftp://gridftp.echo.stfc.ac.uk/lhcb:prod"
  timestr = time.strftime("%d%m%Y-%H%M%S")
  ftsFileList = "FTSList-" + timestr + ".txt"
  print ftsFileList
  fFTS = open(ceBase + "TODO/" + ftsFileList, "w")
  for line in lFiles:
    if len(line) < 10:
      continue
    # TODO : for now assume everything is going to "lhcb:prod"
    fFTS.write(castorPrefix + line + "  " + echoPrefix + line + "\n")
  fFTS.close()

# # TODO : For now hard code the file list. Later we will get this list from the DFC
# # We will also have to make sure somehow that the file is not already handled previously
# testList = [
# "/lhcb/LHCb/Collision10/FULL.DST/00037160/0000/00037160_00005496_1.full.dst",
# "/lhcb/LHCb/Collision10/FULL.DST/00037160/0000/00037160_00005495_1.full.dst"
# ]

# To make sure that the test fails - do not want to touch the production instance even by mistake
# testList = [ "test1.txt", "test2.castor", "test3.dcache", "test4.root", "test5.xroot", "test6.whatever"]

# Get the first nLFNs lines first
com1 = "head -n %s %s" %(str(nLFNs), lfnListFile)
testList = os.popen(com1).read().strip().split("\n")
# Remove the first nLFNs lines from the file
com2 = "sed -i -e '1,%sd' %s" %(str(nLFNs), lfnListFile)
os.system(com2)

writeFileSet(testList)
