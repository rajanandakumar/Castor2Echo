# Castor2Echo
Collection of scripts to ease migration from Castor to Echo at RAL.
Follow the standard procedure of DAQ, with the "files to be transferred" as our target. So, we have :

1. `Producer` (getNextFileSet.py) : Produces a time-stamped file containing the list of files to be transferred from Castor to Echo
2. `Consumer` (checkAndSubmitJob.py) : Picks up the above file, submits it to the (RAL) FTS server and saves the FTS job ID to a sqlite dB
3. `Monitor` (TODO : monitorFTSStatus.py) : Goes over all the open FTS transactions and gives us the status of the transfer request. Should also let us know of failures. Maybe even keep track of the size of the files transferred.

The consumer and monitor python files will in addition import a file describing the structure of the sqlite dB where the IDs and statuses of the FTS jobs are stored.

The above three processes should be run as cron jobs on heplnx005.

