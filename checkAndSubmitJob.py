#!/usr/bin/python
import os
from os import walk
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

# The sqlalchemy magic to get the link to the sqlite db (or create it if needed)
Base = declarative_base()
class ftsjob(Base):
  __tablename__ = 'FTSJobMap'
  file = Column(String(250), primary_key=True)
  ftsid = Column(String(250), nullable=False)

def doTheSQLiteAndGetItsPointer():
  engine = create_engine('sqlite:///sqlite_example.db')
  # Should run only the first time
  if not os.path.exists("sqlite_example.db"):
    Base.metadata.create_all(engine)
  Base.metadata.bind = engine
  session = sessionmaker(bind=engine)()
  return session

# First copy the earliest file over if there is one or more new files to be submitted as an FTS job
def copyFTSFileJob():
  files = []
  for (dirpath, dirnames, filenames) in walk("../TODO/"):
    files.extend(filenames)
    break
  files.remove("getNextFileSet.py")
  files.sort()
  print files
  fileToUse = files[0]
  command = "mv ../TODO/" + fileToUse + " ."
  os.system(command)
  return fileToUse

# Submit the FTS job to the FTS server and retrieve the FTS job ID
def submitTheFTSJob(ftsFile):
  comm = "fts-transfer-submit"
  argument =  "-K -o -s https://lcgfts3.gridpp.rl.ac.uk:8446 --file"
  command = comm + " " + argument + " " + ftsFile
  print command
  ftsJobID = os.popen(command).read()
  return ftsJobID

theFile = copyFTSFileJob()
ftsJobID = submitTheFTSJob(theFile)

#Now I have a pair - write them to the SQLite DB.
sess = doTheSQLiteAndGetItsPointer()
newJob = ftsjob(file=theFile, ftsid=ftsJobID)
sess.add(newJob)
sess.commit()
