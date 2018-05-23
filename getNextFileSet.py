#!/usr/bin/python
import time

# Has to be python 2.6.6 compatible, as that is what I am working with right now

def writeFileSet(lFiles):
  # lFiles is a list of LFNs which I will then write pairwise into a single file for
  # submission as an fts job
  castorPrefix = "srm://srm-lhcb.gridpp.rl.ac.uk:8443/srm/managerv2?SFN=/castor/ads.rl.ac.uk/prod"
  echoPrefix = "gsiftp://gridftp.echo.stfc.ac.uk/lhcb:prod"
  timestr = time.strftime("%d%m%Y-%H%M%S")
  ftsFileList = "FTSList-" + timestr + ".txt"
  print ftsFileList
  fFTS = open(ftsFileList, "w")
  for line in lFiles:
    if len(line) < 10:
      continue
    # TODO : for now assume everything is going to "lhcb:prod"
    fFTS.write(castorPrefix + line + "  " + echoPrefix + line + "\n")
  fFTS.close()

# TODO : For now hard code the file list. Later we will get this list from the DFC
# We will also have to make sure somehow that the file is not already handled previously
# testList = [
# "/lhcb/LHCb/Collision10/FULL.DST/00037160/0000/00037160_00005496_1.full.dst",
# "/lhcb/LHCb/Collision10/FULL.DST/00037160/0000/00037160_00005495_1.full.dst"
# ]
testList = [ "test1.txt", "test2.castor", "test3.dcache", "test4.root", "test5.xroot", "test6.whatever"]

writeFileSet(testList)
