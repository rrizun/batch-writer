#!/bin/sh
echo ${1?}
./gradlew -Dfoo=${1?} run
