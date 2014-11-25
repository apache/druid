A micro cluster consisting of a historical, coordinator, and broker node. Also tries to run zookeeper.

To run, call the microcluster command with the following JVM options:
-server
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
-Xmx4g
-Xms1g

The main class is `io.druid.cli.Main` and the arguments are `microcluster all`

To add a mysql database (instead of the embedded Derby), use something like the following:
-Ddruid.metadata.storage.type=mysql
-Ddruid.metadata.storage.connector.connectURI="jdbc:mysql://localhost:3306/druid"
-Ddruid.metadata.storage.connector.user=druid
-Ddruid.metadata.storage.connector.password=diurd
