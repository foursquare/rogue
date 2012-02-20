#!/bin/bash

# Internal options, always specified
INTERNAL_OPTS="-Dfile.encoding=UTF-8 -Xss8M -Xmx1G -noverify -XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC -XX:MaxPermSize=512M"

# Default options, if nothing is specified
DEFAULT_OPTS=""

# Call with INTERNAL_OPTS followed by SBT_OPTS (or DEFAULT_OPTS). java aways takes the last option when duplicate.
exec java ${INTERNAL_OPTS} ${SBT_OPTS:-${DEFAULT_OPTS}} -jar `dirname $0`/project/sbt-launch.jar "$@"
