---
layout: doc_page
---
# insert-segment-to-db Tool

`insert-segment-to-db` is a tool that can insert segments into Druid metadata storage. It is intended to be used
to update the segment table in metadata storage after people manually migrate segments from one place to another.
It can also be used to insert missing segment into Druid, or even recover metadata storage by telling it where the
segments are stored.

Note: This tool expects users to have Druid cluster running in a "safe" mode, where there are no active tasks to interfere
the segments being inserted. Users can optionally bring down the cluster to make 100% sure nothing is interfering.

In order to make it work, user will have to provide metadata storage credentials and deep storage type through Java JVM argument
or runtime.properties file. Specifically, this tool needs to know

`druid.metadata.storage.type`

`druid.metadata.storage.connector.connectURI`

`druid.metadata.storage.connector.user`

`druid.metadata.storage.connector.password`

`druid.storage.type`

Besides the properties above, you also need to specify the location where the segments are stored and whether you want to
update descriptor.json (`partitionNum_descriptor.json` for HDFS data storage). These two can be provided through command line arguments.

`--workingDir` (Required)

    The directory URI where segments are stored. This tool will recursively look for segments underneath this directory
    and insert/update these segments in metdata storage.
    Attention: workingDir must be a complete URI, which means it must be prefixed with scheme type. For example,
    hdfs://hostname:port/segment_directory

`--updateDescriptor` (Optional)

    if set to true, this tool will update `loadSpec` field in `descriptor.json` (`partitionNum_descriptor.json` for HDFS data storage) if the path in `loadSpec` is different from
    where `desciptor.json` (`partitionNum_descriptor.json` for HDFS data storage) was found. Default value is `true`.

Note: you will also need to load different Druid extensions per the metadata and deep storage you use. For example, if you
use `mysql` as metadata storage and HDFS as deep storage, you should load `mysql-metadata-storage` and `druid-hdfs-storage`
extensions.


Example:

Suppose your metadata storage is `mysql` and you've migrated some segments to a directory in HDFS, and that directory looks
like this,

```
Directory path: /druid/storage/wikipedia

├── 2013-08-31T000000.000Z_2013-09-01T000000.000Z
│   └── 2015-10-21T22_07_57.074Z
│           ├── 0_descriptor.json
│           └── 0_index.zip
├── 2013-09-01T000000.000Z_2013-09-02T000000.000Z
│   └── 2015-10-21T22_07_57.074Z
│           ├── 0_descriptor.json
│           └── 0_index.zip
├── 2013-09-02T000000.000Z_2013-09-03T000000.000Z
│   └── 2015-10-21T22_07_57.074Z
│           ├── 0_descriptor.json
│           └── 0_index.zip
└── 2013-09-03T000000.000Z_2013-09-04T000000.000Z
    └── 2015-10-21T22_07_57.074Z
            ├── 0_descriptor.json
            └── 0_index.zip
```

To load all these segments into `mysql`, you can fire the command below,

```
java 
-Ddruid.metadata.storage.type=mysql 
-Ddruid.metadata.storage.connector.connectURI=jdbc\:mysql\://localhost\:3306/druid 
-Ddruid.metadata.storage.connector.user=druid 
-Ddruid.metadata.storage.connector.password=diurd 
-Ddruid.extensions.loadList=[\"mysql-metadata-storage\",\"druid-hdfs-storage\"] 
-Ddruid.storage.type=hdfs
-cp $DRUID_CLASSPATH 
io.druid.cli.Main tools insert-segment-to-db --workingDir hdfs://host:port//druid/storage/wikipedia --updateDescriptor true
```

In this example, `mysql` and deep storage type are provided through Java JVM arguments, you can optionally put all
of them in a runtime.properites file and include it in the Druid classpath. Note that we also include `mysql-metadata-storage`
and `druid-hdfs-storage` in the extension list.

After running this command, the segments table in `mysql` should store the new location for each segment we just inserted.
Note that for segments stored in HDFS, druid config must contain core-site.xml as described in [Druid Docs](http://druid.io/docs/latest/tutorials/cluster.html), as this new location is stored with relative path.

It is also possible to use `s3` as deep storage. In order to work with it, specify `s3` as deep storage type and load 
[`druid-s3-extensions`](../development/extensions-core/s3.html) as an extension.

```
java
-Ddruid.metadata.storage.type=mysql 
-Ddruid.metadata.storage.connector.connectURI=jdbc\:mysql\://localhost\:3306/druid 
-Ddruid.metadata.storage.connector.user=druid 
-Ddruid.metadata.storage.connector.password=diurd
-Ddruid.extensions.loadList=[\"mysql-metadata-storage\",\"druid-s3-extensions\"]
-Ddruid.storage.type=s3
-Ddruid.s3.accessKey=... 
-Ddruid.s3.secretKey=...
-Ddruid.storage.bucket=your-bucket
-Ddruid.storage.baseKey=druid/storage/wikipedia
-Ddruid.storage.maxListingLength=1000
-cp $DRUID_CLASSPATH
io.druid.cli.Main tools insert-segment-to-db --workingDir "druid/storage/wikipedia" --updateDescriptor true
```

 Note that you can provide the location of segments with either `druid.storage.baseKey` or `--workingDir`. If both are 
 specified, `--workingDir` gets higher priority. `druid.storage.maxListingLength` is to determine the length of a
 partial list in requesting a object listing to `s3`, which defaults to 1000.