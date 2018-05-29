#!/usr/bin/python

# from DIRAC.Core.Base.Script import parseCommandLine
# parseCommandLine()

import time
import random

ceBase = "/afs/cern.ch/work/n/nraja/public/castor2echo/"

# from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
# port = random.choice([9196, 9197,9198, 9199])
# hostname = 'yourmachine.somewhere.something'
# servAddress = 'dips://%s:%s/DataManagement/FileCatalog' % ( hostname, port )
# maxDuration = 1800  # 30mn
# fc = FileCatalogClient(servAddress)

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

# TODO : For now hard code the file list. Later we will get this list from the DFC
# We will also have to make sure somehow that the file is not already handled previously
testList = [
"/lhcb/LHCb/Collision10/FULL.DST/00037160/0000/00037160_00005496_1.full.dst",
"/lhcb/LHCb/Collision10/FULL.DST/00037160/0000/00037160_00005495_1.full.dst"
]

# To make sure that the test fails - do not want to touch the production instance even by mistake
# testList = [ "test1.txt", "test2.castor", "test3.dcache", "test4.root", "test5.xroot", "test6.whatever"]

writeFileSet(testList)
