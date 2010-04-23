#!/bin/bash

###
# Execute an isis command in a shell with the necessary
# environment variables set.
###

NEBULA_ISIS_ROOT="/big/packages/isis3/isis"
LOCAL_ISIS_ROOT="${HOME}/apps/isis3"

if [[ -z $ISISROOT ]]
then
    if [[ -e $NEBULA_ISIS_ROOT ]]
    then
        export ISISROOT=$NEBULA_ISIS_ROOT
    else
        export ISISROOT=$LOCAL_ISIS_ROOT
    fi
fi
echo "ISISROOT is $ISISROOT"
source "${ISISROOT}/scripts/isis3Startup.sh"
echo "LD_LIIBRARY_PATH is $LD_LIBRARY_PATH"
echo "ISIS3DATA is $ISIS3DATA"
command=$ISISROOT/bin/$1
shift
echo "$command $@"
$command "$@"
