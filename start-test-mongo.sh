#!/bin/bash
# Usage: ./start-test-mongo.sh

# EROGU
MONGO_PORT=37648

MY_DIR=$(dirname $0)

# download mongodb if necessary and place it inside the dependencies/ subdirectory.
MONGO_DIR=osx
MONGO_VERSION=2.6.10
MONGO=mongodb-osx-x86_64-$MONGO_VERSION

if [ $(uname) = 'Linux' ]; then
  MONGO_DIR=linux
  MONGO=mongodb-linux-x86_64-$MONGO_VERSION
fi

mkdir -p dependencies

if [ ! -e dependencies/$MONGO ]; then
  echo "Fetching MongoDB: $MONGO"
  curl -# http://fastdl.mongodb.org/$MONGO_DIR/$MONGO.tgz | tar -xz -C dependencies/
fi

# ln -sf doesn't work on mac os x, so we need to rm and recreate.
rm -f dependencies/mongodb
ln -sf $MONGO dependencies/mongodb

if ! [ -d mongo-testdb ]; then
  echo "creating mongo-testdb directly for tests that use mongo"
  mkdir mongo-testdb
fi

if ! ./dependencies/mongodb/bin/mongo --port $MONGO_PORT --eval "db.serverStatus()" 2>&1 > /dev/null; then
  echo "automatically starting up local mongo on $MONGO_PORT so we can use it for tests"
  ./dependencies/mongodb/bin/mongod --dbpath mongo-testdb --maxConns 800 --port $MONGO_PORT $@ 2>&1 | tee mongo.log
else
  echo "great, you have a local mongo running for tests already"
fi
