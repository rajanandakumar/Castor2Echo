# Run in one shot a comparison of the stuff that has been transferred successfully so far

# Define the list of SEs
# Do not sync RAL-BUFFER for now as they change very fast
# RAL-USER (and RAL-BUFFER) need to be done separately as they have different logical paths from
#  (RAL-DST and RAL_MC-DST)
# The following line should be for example
#    declare -a ses=("RAL_MC-DST" "RAL-DST" "RAL-USER" "RAL-BUFFER")
declare -a ses=("RAL-USER")

DATE=`date +%d%b%Y`
workDir="cmp-"$DATE
notCopiedFile="reSync-USER-"$DATE".list"
echo "Running on $DATE"

if [ -d "$workDir" ]; then
    echo "Directory $workDir already exists ... quitting"
    exit -1
fi
mkdir $workDir
cd $workDir

# Define what SE we are sync-ing
echo "Defining the SEs we are re-syncing"
for se in "${ses[@]}"; do
    echo "$se;prod" >> seDef.csv
done

# Get the latest dump from ECHO. Remember to pick up the latest timestamp
cat ${HOME}/.lcgpasswd | lhcb-proxy-init --valid 120:00 --pwstdin
lastMonday=`date -dlast-monday +%Y%m%d`
echo "Getting the dump from ECHO last Monday : $lastMonday"
gfal-copy gsiftp://gridftp.echo.stfc.ac.uk/lhcb:accounting/lhcb/dump_$lastMonday se.csv

# The famous comparison script from Chris Haen
cp /afs/cern.ch/work/n/nraja/public/castor2echo/Re-Sync/cmp-template/cmp-echo-dump.py cmp-echo-dump.py

# Get the current list of files from DFC
for se in "${ses[@]}"; do
    echo "Getting the DFC dump for the SE : $se"
    cmd='mysql -u DiracRO --password=JustLookingAround -h dbod-dfc -P 5506 FileCatalogDB -e "SELECT SEName, CONCAT(d.Name, '\''/'\'', f.FileName) from FC_DirectoryList d JOIN FC_Files f on d.DirID = f.DirID JOIN FC_Replicas r on f.FileID = r.FileID JOIN FC_StorageElements s on s.SEID=r.SEID where SEName in ('\'$se\'')" >> fc.csv'
    eval $cmd
done

# Fix the tabs and convert it into a ; separated file
echo "Converting the DFC dump into a csv"
sed -e '/SEName/d' -e 's/\t/;/g' -i fc.csv

# Run the comparison. Note that the default python (on CVMFS) will fail as it misses
# the IPython.core.interactiveshell. It works from the system installed python though
echo "Actually running the comparison between the DFC and the ECHO dump"
/usr/bin/python cmp-echo-dump.py
cat dataNotCopiedToRAL.csv | awk -F "," '{print $3}' > $notCopiedFile
sed -e '/lfn/d' -i $notCopiedFile
