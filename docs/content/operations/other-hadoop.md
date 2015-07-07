---
layout: doc_page
---
Working with different versions of Hadoop may require a bit of extra work for the time being. We will make changes to support different Hadoop versions in the near future. If you have problems outside of these instructions, please feel free to contact us in IRC or on the [forum](https://groups.google.com/forum/#!forum/druid-development).

Working with Hadoop 2.x
-----------------------
The default version of Hadoop bundled with Druid is 2.3. This should work out of the box.

To override the default Hadoop version, both the Hadoop Index Task and the standalone Hadoop indexer support the parameter `hadoopDependencyCoordinates`. You can pass another set of Hadoop coordinates through this parameter (e.g. You can specify coordinates for Hadoop 2.4.0 as `["org.apache.hadoop:hadoop-client:2.4.0"]`).

The Hadoop Index Task takes this parameter has part of the task JSON and the standalone Hadoop indexer takes this parameter as a command line argument.

If you are still having problems, include all relevant hadoop jars at the beginning of the classpath of your indexing or historical nodes.

Working with CDH
----------------
Members of the community have reported dependency conflicts between the version of Jackson used in CDH and Druid. Currently, our best workaround is to edit Druid's pom.xml dependencies to match the version of Jackson in your hadoop version and recompile Druid.

For more about building Druid, please see [Building Druid](../development/build.html).


Working with Hadoop 1.x and older
---------------------------------
We recommend recompiling Druid with your particular version of Hadoop by changing the dependencies in Druid's pom.xml files. Make sure to also either override the default `hadoopDependencyCoordinates` in the code or pass your Hadoop version in as part of indexing.
