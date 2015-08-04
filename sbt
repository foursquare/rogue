#!/bin/bash

# Internal options, always specified
INTERNAL_OPTS="-Dfile.encoding=UTF-8 -Xss8M -Xmx1G -noverify -XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=128M"

# Default options, if nothing is specified
DEFAULT_OPTS=""

SBT_VERSION="0.12.4"
SBT_LAUNCHER="$(dirname $0)/project/sbt-launch-$SBT_VERSION.jar"

if [ ! -e "$SBT_LAUNCHER" ];
then
    URL="https://repo.typesafe.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/$SBT_VERSION/sbt-launch.jar"
    curl -L -o $SBT_LAUNCHER $URL
fi

# Call with INTERNAL_OPTS followed by SBT_OPTS (or DEFAULT_OPTS). java aways takes the last option when duplicate.
JAVA_BINARY=java
if [ -n "$JAVA_HOME" ]; then
  JAVA_BINARY=$JAVA_HOME/bin/java
fi

$JAVA_BINARY -version 2>&1 | grep -q '1.8'
if [ $? == "0" ]; then
  echo "You are running java 1.8, scala 2.10 requires java 1.7"
  if [ "$(uname)" == "Darwin" ]; then
    echo "On mac, looking for 1.7"
    JAVA_17=$(ls -d -1 /Library/Java/JavaVirtualMachines/** | grep 1.7)
    if [ $JAVA_17 ]; then
      echo "found at $JAVA_17"
      JAVA_HOME=$JAVA_17/Contents/Home/
      JAVA_BINARY=$JAVA_HOME/bin/java
    else
      echo "couldn't find java 1.7, please install"
      exit 1
    fi
  else
    exit 1
  fi
fi

exec $JAVA_BINARY ${INTERNAL_OPTS} ${SBT_OPTS:-${DEFAULT_OPTS}} -jar $SBT_LAUNCHER "$@"
