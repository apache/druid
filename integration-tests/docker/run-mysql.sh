#!/bin/bash

find /var/lib/mysql -type f -exec touch {} \; && /usr/bin/pidproxy /var/run/mysqld/mysqld.pid /usr/bin/mysqld_safe --bind-address=0.0.0.0