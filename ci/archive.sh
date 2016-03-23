#!/bin/bash -ex

pushd `dirname $0`/.. > /dev/null
root=$(pwd -P)
popd > /dev/null

# gather some data about the repo
source $root/ci/vars.sh

# Copy the cloud foundry properties file 
cp $root/mainServiceController/conf/application-cf.properties $root/mainServiceController/src/main/resources/application.properties

# Path to output JAR
src=$root/mainServiceController/target/piazzaServiceController-1.0.0.BUILD-SNAPSHOT.jar
# Build Spring-boot JAR
[ -f $src ] || mvn clean package -U

# stage the artifact for a mvn deploy
mv $src $root/$APP.$EXT
