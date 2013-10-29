#!/usr/bin/python

import os
import time
import signal
import subprocess

processes = [] 

def run():
	global processes
	kafka_path = "../kafka-0.7.2-incubating-src/"
	zookeeper = "bin/zookeeper-server-start.sh config/zookeeper.properties"
	kafka = "bin/kafka-server-start.sh config/server.properties"
	print zookeeper

	zoo = subprocess.Popen(zookeeper.split(" "), cwd=kafka_path)
	time.sleep(1)
	kaf = subprocess.Popen(kafka.split(" "), cwd=kafka_path)

	processes.append(zoo)
	processes.append(kaf)

	druid = "java -Xmx512m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Ddruid.realtime.specFile=realtime.spec -classpath config/realtime:services/target/druid-services-0.5.54-selfcontained.jar com.metamx.druid.realtime.RealtimeMain"

	dru = subprocess.Popen(druid.split(" "))
	processes.append(dru)

	processes[-1].wait()


if __name__ == "__main__":
	os.setpgrp()
	try:
		run()
	except KeyboardInterrupt:
		os.kill(os.getppid(), 0)
		exit(1)
	finally:
		os.killpg(0, signal.SIGKILL)
		
