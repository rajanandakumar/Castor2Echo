#! /usr/bin/env python

# Extract just the lfn from the csv file
import sys
import csv

with open("dataNotCopiedToRAL.csv") as csvfile:
    allLines = csv.reader(csvfile)
    for line in allLines:
        lfn = line[2]
        if len(lfn) < 10: continue
        print lfn
        if not lfn.startswith("/lhcb"):
            print lfn, line
