import argparse
import ConfigParser

from sources import TwitterDataIngestSource
from sources import FacebookDataIngestSource
from sources import S3DataIngestSource
from sources import LocalDataIngestSource
from sources import YoutubeDataIngestSource

from sinks import S3DataIngestSink
from sinks import LocalDataIngestSink
from sinks import SocketDataIngestSink
from sinks import PostgresDataIngestSink
from sinks import MongoDBDataIngestSink


def main():
  parser = argparse.ArgumentParser(
    description = "Data Ingest for the W205 Social Media Monitoring"
  )
  parser.add_argument(
    '--config', help = "path to configuration file", required = True
  )
  args = parser.parse_args()

  config = ConfigParser.ConfigParser()
  config.read(args.config)

  ingest = DataIngest(config)
  
  # Initialize source and sink
  ingest.init_source()
  ingest.init_sink()

  # If one source and one sink is configured, start ingest
  if ingest.sources and ingest.sinks:
    ingest.start_collection()
  else:
    print "Check config settings and retry"

class DataIngest:
  """Flexible source and destination Data Ingest for Social Media"""
  def __init__(self, config):
    self.config = config
    self.sources = [ ]
    self.sinks = [ ]

  # DEFINE METHODS
  # ------------------------------------------------------------------
  def init_source(self):
    if 'Twitter' in self.config.sections():
      print 'Creating Twitter source (found [Twitter] section)'

      twitter_config = dict(self.config.items('Twitter'))
      twitter_source = TwitterDataIngestSource.TwitterDataIngestSource(
        twitter_config
      )

      self.sources.append( twitter_source )
      
    #else:
    #  print "Skipping Twitter since config has no [Twitter] section"

    elif 'Facebook' in self.config.sections():
      print 'Creating Facebook source (found [Facebook] section)'

      facebook_config = dict(self.config.items('Facebook'))
      
      facebook_source = FacebookDataIngestSource.FacebookDataIngestSource(
        facebook_config
      )

      self.sources.append( facebook_source )
    #else:
    #  print "Skipping Facebook since config has no [Facebook] section" 

    elif 'S3Source' in self.config.sections():
      print 'Creating S3 source (found [S3Source] section)'

      s3_source_config = dict(self.config.items('S3Source'))

      s3_source = S3DataIngestSource.S3DataIngestSource(s3_source_config)

      self.sources.append(s3_source)

    #else:
    #  print "Skipping S3 Source since config has no [S3Source] section" 

    elif 'LocalSource' in self.config.sections():
      print 'Creating Local source (found [LocalSource] section)'

      local_source_config = dict(self.config.items('LocalSource'))

      local_source = LocalDataIngestSource.LocalDataIngestSource(
        local_source_config
      )

      self.sources.append(local_source)

    elif 'Youtube' in self.config.sections():
      print 'Creating Youtube source (found [Youtube] section)'

      youtube_source_config = dict(self.config.items('Youtube'))

      youtube_source = YoutubeDataIngestSource.YoutubeDataIngestSource(
        youtube_source_config
      )
      
      self.sources.append(youtube_source)

    #else:
    #  print "Skipping Local Source since config has no [LocalSource] section" 

    else:
      print "No Source found in config file... Stopping DataIngest"
      print "Add a source by including config for one source: Twitter, Facebook, S3Source, or LocalSource"

  # ------------------------------------------------------------------
  def init_sink(self):

    if 'S3' in self.config.sections():
      print 'Creating Local sink (found [S3] section)'

      s3_config = dict(self.config.items('S3'))
      s3_sink = S3DataIngestSink.S3DataIngestSink(s3_config)

      self.sinks.append(s3_sink)
    #else:
    #  print "Skipping S3 since config has no [S3] section"

    elif 'Local' in self.config.sections():
      print 'Creating Local sink (found [Local] section)'

      local_config = dict(self.config.items('Local'))
      local_sink = LocalDataIngestSink.LocalDataIngestSink(local_config)

      self.sinks.append(local_sink)
    #else:
    #  print "Skipping Local since config has no [Local] section"

    elif 'SocketSink' in self.config.sections():
      print 'Creating Socket sink (found [SocketSink] section)'

      socket_config = dict(self.config.items('SocketSink'))
      socket_sink = SocketDataIngestSink.SocketDataIngestSink(socket_config)

      self.sinks.append(socket_sink)
    #else:
    #  print "Skipping Socket since config has no [Socket] section"

    elif 'MongoDB' in self.config.sections():
      print 'Creating MongoDB sink (found [MongoDB] section)'

      mongo_config = dict(self.config.items('MongoDB'))
      mongo_sink = MongoDBDataIngestSink.MongoDBDataIngestSink(mongo_config)

      self.sinks.append(mongo_sink)


    elif 'PostgresSink' in self.config.sections():
      print 'Creating Postgres sink (found [PostgresSink] section)'

      postgres_config = dict(self.config.items('PostgresSink'))
      postgres_sink = PostgresDataIngestSink.PostgresDataIngestSink(postgres_config)

      self.sinks.append(postgres_sink)
    #else:
    #  print "Skipping Postgres since config has no [PostgresSink] section"

    else:
      print "No Sink found in config file... Stopping DataIngest"
      print "Add a sink by including in config a section for: Local, SocketSink, or PostgresSink"

  # ------------------------------------------------------------------
  def start_collection(self):
    self.sinks[0].write(self.sources[0])

if __name__ == "__main__":
  main()

