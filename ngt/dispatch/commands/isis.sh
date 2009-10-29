#!/bin/bash

###
# Execute an isis command in a shell with the necessary
# environment variables set.
###

if [[ -z $ISISROOT ]]
then
    export ISISROOT='/big/packages/isis3/isis'
fi

source "${ISISROOT}/scripts/isis3Startup.sh"
command=$ISISROOT/bin/$1
shift
echo "$command $*"
$command $*
