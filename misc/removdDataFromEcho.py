#! /usr/bin/env python
import os, sys
import tempfile
from itertools import islice
import subprocess, shlex

locDir = "cmp-29Jan2019"
nToDelete = 50
echoServer = "gsiftp://gridftp.echo.stfc.ac.uk"
echoPrefix = echoServer + "/lhcb:"
dirPrefix = "/afs/cern.ch/work/n/nraja/public/castor2echo/Re-Sync/" + locDir + "/removalTmp/delStatus-"
tmpFileName = dirPrefix + next(tempfile._get_candidate_names())
failedDeletion = []

def doTheDeletion(lFiles):
    lFiles = [echoPrefix + line.strip() for line in lFiles]
    longFileList = " ".join(lFiles)
    # delCommand = "gfal-rm --bulk " + longFileList
    delCommand = "gfal-rm -vvv --bulk " + longFileList
    print delCommand
    # os.system(delCommand)
    runComm = subprocess.Popen(shlex.split(delCommand), shell=False, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
    theInfo = runComm.communicate()[0].strip()
    print shlex.split(delCommand)
    print theInfo
    return theInfo
    # return "00000000"

def processDelOutput(dOut):
    bulkStat = dOut.split("\n")
    for stat in bulkStat:
        words = stat.split()
        if len(words)>2:
            print stat
        else:
            lfn = words[0].split("lhcb:")[1]
            status = words[1]
            if status == "MISSING": # Already deleted / gone
                continue
            elif status == "DELETED": # Just deleted
                continue
            elif status == "FAILED":
                print stat
                failedDeletion.append(lfn)
            else: # Unknown ...
                print stat

def deleteBunch(fS, retry=0):
    delStatus = doTheDeletion(fS)
    with open(tmpFileName,'a') as outF:
        outF.write(delStatus + "\n")
    if retry == 1: failedDeletion[:] = []
    processDelOutput(delStatus)

kount = 11
with open("dataToBeRemoved-1.list") as f:
    x = 0
    while True:
        fileSet = list(islice(f, nToDelete))
        if not fileSet:
            break
        x += 1
        print x, len(failedDeletion), nToDelete*(x-1), float(len(failedDeletion))*(x-1)/nToDelete 
        if x > kount:
            # print fileSet
            deleteBunch(fileSet)
        # if len(failedDeletion) > 50:
        #     deleteBunch(failedDeletion, retry=1)
        if x > kount: break
# deleteBunch(failedDeletion, retry=1)
# print failedDeletion
