#!/bin/bash -ex

pushd `dirname $0` > /dev/null
base=$(pwd -P)
popd > /dev/null

# Gather some data about the repo
source $base/vars.sh

# Send a null Job status check
[ curl -H "Content-Type: application/json" -X POST -d '{ "name":"The toLower Service", "description":"Service to convert string to lower case", "url":"http://localhost:8082/jumpstart/string/toLower", "method":"POST", "params": ["aString"], "requestMimeType":"application/json" }' http://http://pz-servicecontroller.cf.piazzageo.io/servicecontroller/registerService = 200 ]
