#! /bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#--------------------------------------------------------------------

# Probably deprecated and to be removed.

# Run from Travis which has no good way to attach logs to a
# build. Instead, we check if any IT failed. If so, we append
# the last 100 lines of each server log to stdout. We have to
# stay wihtin the 4MB limit which Travis applies, so we only
# emit logs for the first failure, and only for servers that
# don't report normal completion.
#
# The only good way to check for test failures is to parse
# the Failsafe summary for each test located in
# <project>/target/failsafe-reports/failsafe-summary.xml
#
# This directory has many subdirectories, some of which are
# tests. We rely on the fact that a test starts with "it-" AND
# contains a failsafe report. (Some projects start with "it-"
# but are not tests.)

# Run in the docker-tests directory
cd $(dirname $0)

# Scan for candidate projects
for PROJECT in it-*
do
	# Check if a failsafe report exists. It will exist if the directory is
	# a test project and failsafe ran on that directory.
	REPORTS="$PROJECT/target/failsafe-reports/failsafe-summary.xml"
	if [ -f "$REPORTS" ]
	then
		# OK, so Bash isn't the world's best text processing language...
		ERRS=1
		FAILS=1
		while IFS= read -r line
	    do
	      	if [ "$line" = "    <errors>0</errors>" ]
	      	then
	      		ERRS=0
	      	fi
	      	if [ "$line" = "    <failures>0</failures>" ]
	      	then
	      		FAILS=0
	      	fi
		done < "$REPORTS"
		if [ $ERRS -eq 1 -o $FAILS -eq 1 ]
	    then
	    	FOUND_LOGS=0
	    	echo "======= $PROJECT Failed =========="
	    	# All logs except zookeeper
	    	for log in $(ls $PROJECT/target/shared/logs/[a-y]*.log)
	    	do
	    		# We assume that a successful exit includes a line with the
	    		# following:
	    		# Stopping lifecycle [module] stage [INIT]
	    		tail -5 "$log" | grep -Fq 'Stopping lifecycle [module] stage [INIT]'
	    		if [ $? -ne 0 ]
	    		then
	    			# Assume failure and report tail
	    		   echo $(basename $log) "logtail ========================"
	    		   tail -100 "$log"
	    		   FOUND_LOGS=1
	    		fi
	        done

	        # Only emit the first failure to avoid output bloat
	        if [ $FOUND_LOGS -eq 1 ]
	        then
	    		exit 0
	    	else
	    		echo "All Druid services exited normally."
	    	fi
	    fi
	fi
done
