###########
rucioevents
###########

This module is designed to generate Kafka events that simulate real events produced by Rucio, enabling the ingestion of files already replicated in a destination butler by ctrl_ingestd.
It need a valid proxy to access Rucio. 
To use it::

    git clone git@github.com:gabrimaine/RucioDummyEventCreator.git
    cd RucioDummyEventCreator/python/lsst/rucioevents
    python dummy_event_generator.py --help

Usage::

    dummy_event_generator.py [-h] (-d DID [DID ...] | -f FILE) -r RSE [-t TOPIC] [-v]

    Process a list of DIDs and send events to Kafka.

  Options:

    -h, --help
        show this help message and exit

    -d DID [DID ...], --dids DID [DID ...]
        List of DIDs in the format "scope:name".

    -f FILE, --file FILE
        Path to a file containing a list of DIDs in the format "scope:name", one per line.

    -r RSE, --rse RSE
        Specify the RSE to be used for processing.

    -t TOPIC, --topic TOPIC
        Specify Kafka topic. Defaults to RSE name if not provided.

    -v, --verbose
        Increase the verbosity level of the output.
