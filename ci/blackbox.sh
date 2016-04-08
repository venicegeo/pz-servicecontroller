#!/bin/bash -ex

pushd `dirname $0` > /dev/null
base=$(pwd -P)
popd > /dev/null

#Run the test
newman -sc $base/tests/PiazzaDevelopment.json.postman_collection
newman -sc $base/tests/pz-register-test.json
newman -sc $base/tests/testServiceControllerRestServicesSeq.json.postman_collection
newman -sc $base/tests/UUID_Logger.json.postman_collection
