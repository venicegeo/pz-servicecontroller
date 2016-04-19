#!/bin/bash -ex

pushd `dirname $0` > /dev/null
base=$(pwd -P)
popd > /dev/null

[ -z "$space" ] && space=stage

envfile=$base/environments/$space.postman_environment

[ -f $envfile ] || { echo "no tests configured for this environment"; exit 0; }

cmd="newman -se $envfile -c"

for f in $(ls -1 $base/postman/*postman_collection); do
  $cmd $f
done
