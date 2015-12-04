#!/bin/bash

SCRIPT_DIR="$(dirname "$0")"
JAVA_OPTS=" -Xmx256m"
java $JAVA_OPTS -classpath "$SCRIPT_DIR/../lib/*" -Dlogback.configurationFile=$SCRIPT_DIR/../config/logback.xml dk.statsbiblioteket.metadatarepository.xmltapes.common.index.IndexMigratorRunnable "$@"