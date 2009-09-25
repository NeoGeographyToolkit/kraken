#!/bin/bash
# download and process PDS label products

DIR=$1
# augment path
SCRIPT_DIR=$2

# use a specific python version
PYTHON=/opt/asani/apache/bin/python

# download files from urls
if [ -d "$DIR" ]; then
  wget -nc --directory-prefix=$DIR --tries=3 --connect-timeout=10 --read-timeout=30 --continue -a ${DIR}/progress.log -i ${DIR}/urls.csv
  tar -zcf ${DIR}.tgz $DIR
  rm -rf ${DIR}
  # test if download went well
  #if [ -f "${DIR}.tgz" ]; then # test for empty files
    # TMPSIZE=$(stat -z%s "$TMP")
    # scan pds label files
    # TODO: improve on error reporting!
    ${PYTHON} ${SCRIPT_DIR}/analyzeWgetReport.py ${DIR}.tgz
    ${PYTHON} ${SCRIPT_DIR}/pdstar2pickle.py ${DIR}.tgz
    rm ${DIR}.tgz
  #fi
fi

# return success code
exit 0
