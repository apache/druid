#!/usr/bin/env bash
set +u
shopt -s xpg_echo
shopt -s expand_aliases

PF=./twitter4j.properties

# if twitter4j.properties already exists, then user is okay with having twitter pw in file.
#  Otherwise a twitter4j.properties file in curr. dir. is made temporarily for twitter login.
if [ ! -e "$PF" ]; then
    PF_CLEANUP="/bin/rm $PF"
    trap "${PF_CLEANUP} ; exit 1" 1 2 3 15
    touch created
    touch $PF
    chmod 700 $PF
    echo "   Your twitter user login name and pw is needed "
    read -p 'twitter username? '  TWIT_USER
    read -s -p 'twitter password? ' TWIT_PW
    echo "user=${TWIT_USER}" >> $PF
    echo "password=${TWIT_PW}" >> $PF
    TWIT_PW=""
fi