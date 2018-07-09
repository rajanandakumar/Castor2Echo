#!/bin/bash

. /afs/cern.ch/group/z5/group_bashrc
export X509_USER_PROXY=/afs/cern.ch/user/n/nraja/.globus/c2eMig.temp
export X509_CERT_DIR=/etc/grid-security/certificates
export X509_VOMS_DIR=/etc/grid-security/vomsdir

source /afs/cern.ch/work/n/nraja/public/castor2echo/c2eMigration/bin/activate
counter=0
while [ $counter -le 40 ]
do
    sleep 1
    # The work of the producer
    cd /afs/cern.ch/work/n/nraja/public/castor2echo
    # Uncomment the next line to get the transfers working again
    # python /afs/cern.ch/work/n/nraja/public/castor2echo/TODO/getNextFileSet.py
    # The work of the consumer
    cd /afs/cern.ch/work/n/nraja/public/castor2echo/DOING/
    python /afs/cern.ch/work/n/nraja/public/castor2echo/DOING/checkAndSubmitJob.py
    ((counter++))
done
deactivate
