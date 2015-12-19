import argparse
import ConfigParser

import TwitterDataIngestSource
import S3DataIngestSource
import LocalDataIngestSource

import S3DataIngestSink
import LocalDataIngestSink
import SocketDataIngestSink
import PostgresDataIngestSink


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
  ingest.start()

class DataIngest:
  """Flexible source and destination Data Ingest for Social Media"""
  def __init__(self, config):
    self.config = config
    self.sources = [ ]
    self.sinks = [ ]

  def start(self):
    if 'Twitter' in self.config.sections():
      print 'Creating Twitter source (found [Twitter] section)'

      twitter_config = dict(self.config.items('Twitter'))
      twitter_source = TwitterDataIngestSource.TwitterDataIngestSource(
        twitter_config
      )

      self.sources.append( twitter_source )
      
    else:
      print "Skipping Twitter since config has no [Twitter] section"

    if 'Facebook' in self.config.sections():
      print 'Creating Facebook source (found [Facebook] section)'

      facebook_config = dict(self.config.items('Facebook'))
      print facebook_config
    else:
      print "Skipping Facebook since config has no [Facebook] section" 

    if 'S3Source' in self.config.sections():
      print 'Creating S3 source (found [S3Source] section)'

      s3_source_config = dict(self.config.items('S3Source'))

      s3_source = S3DataIngestSource.S3DataIngestSource(s3_source_config)

      self.sources.append(s3_source)

    else:
      print "Skipping S3 Source since config has no [S3Source] section" 

    if 'LocalSource' in self.config.sections():
      print 'Creating Local source (found [LocalSource] section)'

      local_source_config = dict(self.config.items('LocalSource'))

      local_source = LocalDataIngestSource.LocalDataIngestSource(
        local_source_config
      )

      self.sources.append(local_source)

    else:
      print "Skipping Local Source since config has no [LocalSource] section" 

    if 'S3' in self.config.sections():
      print 'Creating Local sink (found [S3] section)'

      s3_config = dict(self.config.items('S3'))
      s3_sink = S3DataIngestSink.S3DataIngestSink(s3_config)

      self.sinks.append(s3_sink)
    else:
      print "Skipping S3 since config has no [S3] section"

    if 'Local' in self.config.sections():
      print 'Creating Local sink (found [Local] section)'

      local_config = dict(self.config.items('Local'))
      local_sink = LocalDataIngestSink.LocalDataIngestSink(local_config)

      self.sinks.append(local_sink)
    else:
      print "Skipping Local since config has no [Local] section"

    if 'SocketSink' in self.config.sections():
      print 'Creating Socket sink (found [SocketSink] section)'

      socket_config = dict(self.config.items('SocketSink'))
      socket_sink = SocketDataIngestSink.SocketDataIngestSink(socket_config)

      self.sinks.append(socket_sink)
    else:
      print "Skipping Socket since config has no [Socket] section"

    if 'PostgresSink' in self.config.sections():
      print 'Creating Postgres sink (found [PostgresSink] section)'

      postgres_config = dict(self.config.items('PostgresSink'))
      postgres_sink = PostgresDataIngestSink.PostgresDataIngestSink(postgres_config)

      self.sinks.append(postgres_sink)
    else:
      print "Skipping Postgres since config has no [PostgresSink] section"

    self.sinks[0].write(self.sources[0])

if __name__ == "__main__":
  main()

