#!/bin/bash

echo Waiting for postgress...
while ! nc -z postgres 5432; do sleep 1; done

psql -U postgres -h postgres -c "create user druid with password 'diurd'"
psql -U postgres -h postgres -c "create database druid with owner druid"

echo Waiting for kafka...
while ! nc -z kafka 9092; do sleep 1; done

/usr/bin/supervisord -c supervisord.conf &

echo Waiting for supervisor...
while ! nc -z druid 8081; do sleep 1; done

wait