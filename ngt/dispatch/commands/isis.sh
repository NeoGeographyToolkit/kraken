#!/bin/bash

###
# Execute an isis command in a shell with the necessary
# environment variables set.
###

if [[ -z $ISISROOT ]]
then
    export ISISROOT='/big/packages/isis3/isis'
    #export ISISROOT='/opt/isis3/isis'
fi
echo "ISISROOT is $ISISROOT"
source "${ISISROOT}/scripts/isis3Startup.sh"
echo "LD_LIIBRARY_PATH is $LD_LIBRARY_PATH"
command=$ISISROOT/bin/$1
shift
echo "$command $@"
$command "$@"
