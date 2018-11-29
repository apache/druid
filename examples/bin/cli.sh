#!/bin/sh

java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 \
        -cp lib/*:config/_common \
        io.druid.cli.Main $* \
                | grep -v " INFO \[main\] io.druid.initialization.Initialization "

