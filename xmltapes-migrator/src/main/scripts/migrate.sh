#!/bin/bash

SCRIPT_DIR=$(dirname $(readlink -f $BASH_SOURCE[0]))
JAVA_OPTS=" -Xmx256m"
java $JAVA_OPTS -classpath "$SCRIPT_DIR/../config:$SCRIPT_DIR/../lib/*" dk.statsbiblioteket.metadatarepository.xmltapes.migrator.IndexMigratorRunnable "$SCRIPT_DIR/../config/migrator.properties" "$@"