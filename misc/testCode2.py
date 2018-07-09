#!/usr/bin/env python
import os, sys, glob, shutil, random
ceBase = "/afs/cern.ch/work/n/nraja/public/castor2echo/"
sys.path.append(ceBase)

toRenameFiles = glob.glob(ceBase + "DOING/RALSpecific/*.txt")
for ff in toRenameFiles:
  tFN = ff.split("/")[-1]
  if tFN.startswith("FTSList"):
    continue
  else:
    tmpFN = "CFTSList" + tFN.split("FTSList")[-1]
    print tFN, tmpFN