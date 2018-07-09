#!/bin/bash

. /afs/cern.ch/group/z5/group_bashrc
export X509_USER_PROXY=/afs/cern.ch/user/n/nraja/.globus/c2eMig.temp
export X509_CERT_DIR=/etc/grid-security/certificates
export X509_VOMS_DIR=/etc/grid-security/vomsdir
lb-run -c best LHCbDirac/prod bash -f << EEEE
cat ${HOME}/.lcgpasswd | lhcb-proxy-init --valid 24:00 --pwstdin
EEEE

cd /afs/cern.ch/work/n/nraja/public/castor2echo/DONE/
source /afs/cern.ch/work/n/nraja/public/castor2echo/c2eMigration/bin/activate
python /afs/cern.ch/work/n/nraja/public/castor2echo/DONE/monitorFTSStatus.py | tee status.log
deactivate
