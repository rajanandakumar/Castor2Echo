#!/usr/bin/env python

# from DIRAC.Core.Base.Script import parseCommandLine
# parseCommandLine()

import os, sys, glob, shutil, random
import subprocess
import time
ceBase = "/afs/cern.ch/work/n/nraja/public/castor2echo/"
c2eJobFiles = glob.glob(ceBase + "TODO/*.txt")
if len(c2eJobFiles) < 1 :
    print "No files seen!"

for c2eFile in c2eJobFiles:
    goodFiles = []
    badFiles = []
    with open(c2eFile) as toCheck:
        for tup in toCheck:
            fFrom = tup.split()[0]
            comm = "gfal-stat " + fFrom
            runComm = subprocess.Popen(comm, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
            theInfo = runComm.communicate()[1].strip()
            if theInfo.startswith("gfal-stat error: 2 (No such file or directory)"):
                badFiles.append(tup)
            else:
                goodFiles.append(tup)
    os.remove(c2eFile) # Always remove it and re-write into the file
    if len(goodFiles)>0:
        fFTS = open(c2eFile, "w")
        for line in goodFiles:
            if len(line) < 10:
                continue
            fFTS.write(line)
        fFTS.close()
    if len(badFiles)>0:
        fFTS = open(ceBase + "DONE/badFileList.txt", "a")
        for line in badFiles:
            if len(line) < 10:
                continue
            fFTS.write(line)
        fFTS.close()

    print len(goodFiles), len(badFiles)
    print c2eFile
    # if len(badFiles)>0: break
