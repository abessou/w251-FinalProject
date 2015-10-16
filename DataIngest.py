import argparse
import ConfigParser

import TwitterDataIngestSource


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
      twitter_config = dict(self.config.items('Twitter'))
      twitter_source = TwitterDataIngestSource.TwitterDataIngestSource(
        twitter_config
      )

      self.sources.append( twitter_source )
      
    else:
      print "Skipping Twitter since config has no [Twitter] section"

    if 'Facebook' in self.config.sections():
      facebook_config = dict(self.config.items('Facebook'))
      print facebook_config
    else:
      print "Skipping Facebook since config has no [Facebook] section" 

    # todo: multi-source blending generators
    for item in self.sources[0]:
      print item

if __name__ == "__main__":
  main()

