#!/usr/bin/env bash
set +u
shopt -s xpg_echo
shopt -s expand_aliases

PF=./twitter4j.properties

# if twitter4j.properties already existed, then user is okay with having twitter pw in file, don't remove

if [ -e "created" ]; then
    rm -f ${PF}
    rm -f created
fi
