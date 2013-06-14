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
    echo "   Your twitter OAuth information is needed. Go to https://twitter.com/oauth_clients/new to register a new application and retrieve your keys "
    read -p 'Twitter consumer key? ' CONSUMER_KEY
    read -p 'Twitter consumer secret? ' CONSUMER_SECRET
    read -p 'Twitter access token? ' ACCESS_TOKEN
    read -p 'Twitter access token secret? ' ACCESS_TOKEN_SECRET
    echo "debug=true" >> $PF
    echo "oauth.consumerKey=${CONSUMER_KEY}" >> $PF
    echo "oauth.consumerSecret=${CONSUMER_SECRET}" >> $PF
    echo "oauth.accessToken=${ACCESS_TOKEN}" >> $PF
    echo "oauth.accessTokenSecret=${ACCESS_TOKEN_SECRET}" >> $PF
    CONSUMER_SECRET=""
    ACCESS_TOKEN_SECRET=""
fi