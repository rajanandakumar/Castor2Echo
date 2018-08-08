# Castor2Echo
Collection of scripts to ease migration from Castor to Echo at RAL.
Follow the standard (classical) procedure of DAQ, with the "files to be transferred" as our target.

The (minimal) structure of the directory is below. Everything without an extension (sh or py) is a directory
```
├── cronjobs
│   ├── consumer.sh
│   ├── monitor.sh
│   └── producer.sh
├── DOING
│   ├── checkAndSubmitJob.py
├── DONE
│   ├── Bad
│   ├── Dirty
│   ├── monitorFTSStatus.py
│   ├── Okay
├── ftsJob.py
└── TODO
    ├── getNextFileSet.py
    └── ListOfLFNs
```
We have the following files which drive the copy:

1. `Producer` (getNextFileSet.py) : Produces a time-stamped file containing the list of files to be transferred from Castor to Echo and saves it in the directory "TODO"
2. `Consumer` (checkAndSubmitJob.py) : Picks up upto 5 files in the "TODO" directory, submits it to either the RAL or CERN FTS server (50% probability) and saves the FTS job ID to a sqlite dB. Then moves the file to the "DOING" directory.
3. `Monitor` (monitorFTSStatus.py) : Goes over all the open FTS transactions (files in the "DOING" directory) and processes their statuses. Below for more details

The consumer and monitor python files will in addition import a file (ftsJob.py) describing the structure of the sqlite dB where the IDs and statuses of the FTS jobs are stored.

The above three processes should be run as cron jobs on lxplus7, using virtualenv to correctly set the FTS3 environment (the FTS3 rest interface does not run on sl6 due to the python version available).

1. The producer and consumer are run from within a single "consumer.sh" cron script to run once every 2 hours. This is because each of the producer and consumer scripts acts on a single file. Each transfer attempts 20 LFNs currently (configurable in getNextFileSet.py)
2. The Monitor script looks at all pending transfers. It is run every 2 hours.

Other scripts (in the "misc" directory) are written to perform one-time actions as needed.

## Monitor script
The monitoring script checks and performs the following actions :
1. Job not submitted to FTS : Basically no record of job in the local sqlite dB. Move to "TODO" directory and let it get submitted eventually.
2. FINISHED : Move list of files from "DOING" to "DONE/Okay"
3. FINISHEDDIRTY : Extract files which finished well and write to "DONE/Okay". Extract bad files (Transfer error "500 No such file or directory") and write to "DONE/bad" for processing by hand. Move rest of files to "TODO" for re-trying the transfer, with a prefix "D" added to the file name to monitor the request by hand.
4. ACTIVE : Do nothing
5. FAILED : If pathological (>3 failures), move to "DONE/Bad" for processing by hand. Otherwise, retry by moving to "TODO"
6. SUBMITTED : Do nothing
7. All other cases : Print out a message (which comes in the cron log) and do nothing.