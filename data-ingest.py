import argparse
import ConfigParser
import TwitterDataIngest


class DataIngest:
  


    twitter_config = dict(config.items('Facebook')) #('Twitter', 'consumer_key')

    print twitter_config

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
  
if __name__ == "__main__":
  main()

