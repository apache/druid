# IntelliJ Setup
This document contains some examples and instructions on how to get IntelliJ setup to run local debugging and test setups of Druid.

## Code Style
The Code Style is available in XML format at [druid_intellij_formatting.xml](druid_intellij_formatting.xml) and can be [imported into IntelliJ](https://www.jetbrains.com/help/idea/2017.1/copying-code-style-settings.html).

## Metadata
The installation of a MySQL metadata store is outside the scope of this document, but instructions on setting up MySQL can be found at [docs/content/development/extensions-core/mysql.md](docs/content/development/extensions-core/mysql.md). This assumes you followed the example there and have a database named `druid` with proper permissions for a user named `druid` and a password of `diurd`.

Use of other databases such as Postgres or Derby are entirely reasonable, but doing so is left as an excercise to the reader.

## ZooKeeper
This also assumes you have [ZooKeeper](http://zookeeper.apache.org/releases.html) running locally, which usually just involves downloading the latst distribution of ZooKeeper, doing some minor configuration in ZooKeeper's `conf/` directory (most defaults are fine), then running `./bin/zkServer.sh start` in the ZooKeeper directory. 

## Initial Build
Before running the apps, you should do a `mvn clean install -DskipTests` in the Druid source in order to make sure directories are populated correctly.

## XML App Def
You can configure application definitions in XML for import into IntelliJ. Below are a few examples. These should be placed in an XML file in [.idea/runConfigurations](.idea/runConfigurations) in the Druid source code.

### Historical.xml
```xml
<component name="ProjectRunConfigurationManager">
  <configuration default="false" name="Historical" type="Application" factoryName="Application">
    <extension name="coverage" enabled="false" merge="false" sample_coverage="true" runner="idea" />
    <option name="MAIN_CLASS_NAME" value="io.druid.cli.Main" />
    <option name="VM_PARAMETERS" value="-server -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Xmx2G -XX:MaxJavaStackTraceDepth=9999 -XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintAdaptiveSizePolicy -XX:+PrintReferenceGC -verbose:gc -XX:+PrintFlagsFinal -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager -Dorg.jboss.logging.provider=slf4j -Dlog4j.configurationFile=$PROJECT_DIR$/common/src/main/resources/log4j2.debug.xml -Ddruid.host=localhost -Ddruid.service=historical -Ddruid.server.maxSize=10000000000 -Ddruid.processing.buffer.sizeBytes=100000000 -Ddruid.extensions.hadoopDependenciesDir=$PROJECT_DIR$/distribution/target/hadoop-dependencies/ -Ddruid.extensions.directory=$PROJECT_DIR$/distribution/target/extensions/ -Ddruid.extensions.loadList=[\&quot;druid-s3-extensions\&quot;,\&quot;druid-histogram\&quot;,\&quot;mysql-metadata-storage\&quot;] -Ddruid.historical.cache.useCache=false -Ddruid.historical.cache.populateCache=false -Ddruid.segmentCache.locations=&quot;[{\&quot;path\&quot;:\&quot;/tmp/druid/indexCache\&quot;,\&quot;maxSize\&quot;:10000000000}]&quot; -Ddruid.zk.service.host=localhost -Ddruid.processing.numThreads=1 -Ddruid.server.http.numThreads=50 -Ddruid.announcer.type=batch -Ddruid.emitter=logging" />
    <option name="PROGRAM_PARAMETERS" value="server historical" />
    <option name="WORKING_DIRECTORY" value="file://$PROJECT_DIR$" />
    <option name="ALTERNATIVE_JRE_PATH_ENABLED" value="false" />
    <option name="ALTERNATIVE_JRE_PATH" value="1.8" />
    <option name="ENABLE_SWING_INSPECTOR" value="false" />
    <option name="ENV_VARIABLES" />
    <option name="PASS_PARENT_ENVS" value="true" />
    <module name="druid-services" />
    <envs />
    <method />
  </configuration>
</component>
```

### Coordinator.xml
```xml
<component name="ProjectRunConfigurationManager">
  <configuration default="false" name="Coordinator" type="Application" factoryName="Application">
    <extension name="coverage" enabled="false" merge="false" sample_coverage="true" runner="idea" />
    <option name="MAIN_CLASS_NAME" value="io.druid.cli.Main" />
    <option name="VM_PARAMETERS" value="-server -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Xmx256M -Xmx256M -XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintAdaptiveSizePolicy -XX:+PrintReferenceGC -verbose:gc -XX:+PrintFlagsFinal -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager -Dorg.jboss.logging.provider=slf4j -Ddruid.host=localhost -Ddruid.service=coordinator -Ddruid.extensions.directory=$PROJECT_DIR$/distribution/target/extensions/ -Ddruid.extensions.loadList=[\&quot;druid-s3-extensions\&quot;,\&quot;druid-histogram\&quot;,\&quot;mysql-metadata-storage\&quot;] -Ddruid.zk.service.host=localhost -Ddruid.metadata.storage.type=mysql -Ddruid.metadata.storage.connector.connectURI=&quot;jdbc:mysql://localhost:3306/druid&quot; -Ddruid.metadata.storage.connector.user=druid -Ddruid.metadata.storage.connector.password=diurd -Ddruid.announcer.type=batch -Ddruid.emitter=logging -Ddruid.coordinator.period=PT10S -Ddruid.coordinator.startDelay=PT5S" />
    <option name="PROGRAM_PARAMETERS" value="server coordinator" />
    <option name="WORKING_DIRECTORY" value="file://$PROJECT_DIR$" />
    <option name="ALTERNATIVE_JRE_PATH_ENABLED" value="false" />
    <option name="ALTERNATIVE_JRE_PATH" value="1.8" />
    <option name="ENABLE_SWING_INSPECTOR" value="false" />
    <option name="ENV_VARIABLES" />
    <option name="PASS_PARENT_ENVS" value="true" />
    <module name="druid-services" />
    <envs />
    <method />
  </configuration>
</component>
```
