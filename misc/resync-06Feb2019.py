#!/usr/bin/env python
import os, sys, datetime
# Do the synchronisation
runCommand = True

# Define the list of SEs
# RAL-BUFFER and RAL-USER change very fast due to deletions
# RAL-USER and RAL-BUFFER need to be done separately as they have different logical paths from
#  (RAL-DST and RAL_MC-DST)
# The following line should be for example
#    ses=("RAL_MC-DST" "RAL-DST" "RAL-USER" "RAL-BUFFER")
ses = [ "RAL-BUFFER" ]

allSEs = { "RAL-DST":"prod", "RAL-BUFFER":"buffer", "RAL-USER":"user", "RAL_MC-DST":"prod", "RAL-FAILOVER":"failover" }
today = datetime.date.today()
lastMonday = today - datetime.timedelta(days=today.weekday())

DATE = today.strftime('%d%b%Y')
workDir = "cmp-" + DATE
notCopiedFile = "reSync-USER-" + DATE + ".list"
print "Running on ", DATE

if os.path.isdir(workDir):
    print "Directory ", workDir, " already exists. Exiting"
    if runCommand: sys.exit(-1)
else:
    print "Making working directory : ", workDir
    comm = "mkdir " + workDir
    if runCommand: os.system(comm)

# Now assume that the directory has been freshly created
try:
    os.chdir(workDir)
except Exception, e:
    print "Exception doing a chdir : ", str(e)

# Define what SE we are sync-ing
print "Defining the SEs we are re-syncing"
seDef_file = open("seDef.csv", "w")
for se in ses:
    bucket = "prod"
    if "USER" in se : bucket = "user"
    elif "BUFFER" in se : bucket = "buffer"
    seDef_file.write(se + ";" + bucket + "\n")
seDef_file.close()

# Get the latest dump from ECHO. Remember to pick up the latest timestamp
comm = "cat ${HOME}/.lcgpasswd | lhcb-proxy-init --valid 2:00 --pwstdin"
if runCommand: os.system(comm)
lm = lastMonday.strftime('%Y%m%d')
print "Getting the dump from ECHO last Monday : ", lm
comm = "gfal-copy gsiftp://gridftp.echo.stfc.ac.uk/lhcb:accounting/lhcb/dump_" + lm + " se.csv"
print comm
if runCommand: os.system(comm)

# The famous comparison script from Chris Haen
comm = "cp /afs/cern.ch/work/n/nraja/public/castor2echo/Re-Sync/cmp-template/cmp-echo-dump.py cmp-echo-dump.py"
if runCommand: os.system(comm)

# Get the current list of files from DFC
for se in ses:
    print "Getting the DFC dump for the SE : ", se
    comm = """mysql -u DiracRO --password=JustLookingAround -h dbod-dfc -P 5506 FileCatalogDB -e "SELECT SEName, CONCAT(d.Name, '/', f.FileName) from FC_DirectoryList d JOIN FC_Files f on d.DirID = f.DirID JOIN FC_Replicas r on f.FileID = r.FileID JOIN FC_StorageElements s on s.SEID=r.SEID where SEName in ('%s')" >> fc.csv""" %se
    print comm
    if runCommand: os.system(comm)

# Fix the tabs and convert it into a ; separated file
print "Converting the DFC dump into a csv"
comm = """sed -e '/SEName/d' -e 's/\t/;/g' -i fc.csv"""
if runCommand: os.system(comm)

# # Run the comparison. Note that the default python (on CVMFS) will fail as it misses
# # the IPython.core.interactiveshell. It works from the system installed python though
print "Actually running the comparison between the DFC and the ECHO dump"
comm = "/usr/bin/python cmp-echo-dump.py"
if runCommand: os.system(comm)
comm = """cat dataNotCopiedToRAL.csv | awk -F "," '{print $3}' > %s""" %notCopiedFile
if runCommand: os.system(comm)
comm = """sed -e '/lfn/d' -i %s""" %notCopiedFile
if runCommand: os.system(comm)
# Also need to fix the Files to be deleted by removing the SEs that are not synced here
remGrep = " | grep -v accounting"
for se,bucket in allSEs.iteritems():
    if se in ses: continue
    remGrep = remGrep + " | grep -v " + bucket
comm = "cat dataToBeRemoved.csv" + remGrep + " > dataToBeRemoved-1.list"
if runCommand: os.system(comm)
comm = "ls -la"
if runCommand: os.system(comm)
print "All done - bye bye ..."
