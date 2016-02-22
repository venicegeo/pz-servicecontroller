#!/bin/bash -ex

pushd `dirname $0` > /dev/null
base=$(pwd -P)
popd > /dev/null

# Gather some data about the repo
source $base/vars.sh

#Run the test
newman -sc $base/tests/pz-register-test.json
newman -sc $base/tests/testServiceControllerRestServicesSeq.json.postman_collection
newman -sc $base/tests/PiazzaAWM%20Development.json.postman_collection