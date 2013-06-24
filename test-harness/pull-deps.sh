# Check that curl is installed
type curl >/dev/null 2>&1 || { echo >&2 "I require curl but it's not installed. Aborting."; exit 1; }

# Download dependencies
curl http://repo1.maven.org/maven2/org/apache/avro/avro/1.7.4/avro-1.7.4.jar                                        -o kafka/lib/avro-1.7.4.jar
curl http://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar                  -o kafka/lib/commons-logging-1.1.3.jar
curl http://repo1.maven.org/maven2/com/facebook/hadoop/hadoop-core/0.20.0/hadoop-core-0.20.0.jar                    -o kafka/lib/hadoop-core-0.20.0.jar
curl http://repo1.maven.org/maven2/org/codehaus/jackson/jackson-core-asl/1.9.12/jackson-core-asl-1.9.12.jar         -o kafka/lib/jackson-core-asl-1.9.12.jar
curl http://repo1.maven.org/maven2/org/codehaus/jackson/jackson-mapper-asl/1.9.12/jackson-mapper-asl-1.9.12.jar     -o kafka/lib/jackson-mapper-asl-1.9.12.jar
curl http://repo1.maven.org/maven2/net/sf/jopt-simple/jopt-simple/4.5/jopt-simple-4.5.jar                           -o kafka/lib/jopt-simple-4.5.jar
curl http://repo1.maven.org/maven2/junit/junit/4.11/junit-4.11.jar                                                  -o kafka/lib/junit-4.11.jar
curl http://repo1.maven.org/maven2/log4j/log4j/1.2.16/log4j-1.2.16.jar                                              -o kafka/lib/log4j-1.2.16.jar
curl http://repo1.maven.org/maven2/org/apache/pig/pig/0.11.1/pig-0.11.1.jar                                         -o kafka/lib/pig-0.11.1.jar
curl http://repo1.maven.org/maven2/org/scala-lang/scala-library/2.11.0-M3/scala-library-2.11.0-M3.jar               -o kafka/lib/scala-library-2.11.0-M3.jar
curl http://repo1.maven.org/maven2/com/101tec/zkclient/0.3/zkclient-0.3.jar                                         -o kafka/lib/zkclient-0.3.jar
curl http://repo1.maven.org/maven2/org/apache/hadoop/zookeeper/3.3.1/zookeeper-3.3.1.jar                            -o kafka/lib/zookeeper-3.3.1.jar

# Move druid jar
cp ../services/target/druid-services-*-selfcontained.jar druid/
