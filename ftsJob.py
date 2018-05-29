import os
from sqlalchemy import Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
ceBase = "/home/ppd/nraja/castorToEcho/"

# The sqlalchemy magic to get the link to the sqlite db (or create it if needed)
Base = declarative_base()
class ftsjob(Base):
  __tablename__ = 'FTSJobMap'
  ftsFile = Column("FTS file name", String(250), primary_key=True)
  ftsID = Column("FTS job ID", String(250), nullable=False)
  ftsStatus = Column("FTS job status", String(250), nullable=False)
  ftsIter = Column("Number of non-successful times", Integer, nullable=False) # 0 when submitted. To be incremented at every monitoring step

def doTheSQLiteAndGetItsPointer():
  engine = create_engine('sqlite:///' + ceBase + 'sqlite_example.db')
  # Should run only the first time
  if not os.path.exists("sqlite_example.db"):
    Base.metadata.create_all(engine)
  Base.metadata.bind = engine
  session = sessionmaker(bind=engine)()
  return session
