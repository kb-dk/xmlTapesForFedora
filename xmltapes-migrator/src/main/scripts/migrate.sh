#!/bin/bash

SCRIPT_DIR=$(dirname $(readlink -f $BASH_SOURCE[0]))
JAVA_OPTS=" -Xmx256m"
CONFDIR=$1
shift
java $JAVA_OPTS -classpath "$CONFDIR:$SCRIPT_DIR/../lib/*" \
    dk.statsbiblioteket.metadatarepository.xmltapes.migrator.IndexMigratorRunnable "$CONFDIR/migrator.properties" "$@"