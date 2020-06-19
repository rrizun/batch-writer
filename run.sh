#!/bin/sh
echo ${1?}
gradle -Dfoo=${1?} run
