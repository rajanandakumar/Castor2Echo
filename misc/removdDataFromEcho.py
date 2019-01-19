#! /usr/bin/env python
import os, sys
import tempfile
from itertools import islice
import subprocess

echoPrefix = "gsiftp://gridftp.echo.stfc.ac.uk/lhcb:"
dirPrefix = "/afs/cern.ch/work/n/nraja/public/castor2echo/Re-Sync/cmp-19Dec2018/removalTmp/delStatus-"
tmpFileName = dirPrefix + next(tempfile._get_candidate_names())
failedDeletion = []

def doTheDeletion(lFiles):
    lFiles = [echoPrefix + line.strip() for line in lFiles]
    longFileList = " ".join(lFiles)
    # delCommand = "gfal-rm --bulk " + longFileList
    delCommand = "gfal-rm " + longFileList
    # print delCommand
    # os.system(delCommand)
    runComm = subprocess.Popen(delCommand, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
    theInfo = runComm.communicate()[0].strip()
    # print theInfo
    return theInfo

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

with open("dataToBeRemoved-DST-MCDST.list") as f:
    x = 0
    while True:
        fileSet = list(islice(f, 100))
        if not fileSet:
            break
        x += 1
        print x, len(failedDeletion), 100*(x-1), float(len(failedDeletion))/100*(x-1) 
        deleteBunch(fileSet)
        if len(failedDeletion) > 50:
            deleteBunch(failedDeletion, retry=1)
        # if x>100: break
deleteBunch(failedDeletion, retry=1)
print failedDeletion



    # for line in f:
    #     pfn = echoPrefix + line.strip()
    #     command = "gfal-rm "
    #     os.system(command + pfn)
    #     # tmpFileName = dirPrefix + next(tempfile._get_candidate_names())
    #     # print tmpFileName
    #     x += 1
    #     if x>100 : sys.exit()

