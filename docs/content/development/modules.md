---
layout: doc_page
---

# Extending Druid With Custom Modules

Druid uses a module system that allows for the addition of extensions at runtime.

## Writing your own extensions

Druid's extensions leverage Guice in order to add things at runtime.  Basically, Guice is a framework for Dependency Injection, but we use it to hold the expected object graph of the Druid process.  Extensions can make any changes they want/need to the object graph via adding Guice bindings.  While the extensions actually give you the capability to change almost anything however you want, in general, we expect people to want to extend one of the things listed below.  This means that we honor our [versioning strategy](./versioning.html) for changes that affect the interfaces called out on this page, but other interfaces are deemed "internal" and can be changed in an incompatible manner even between patch releases.

1. Add a new deep storage implementation
1. Add a new Firehose
1. Add Aggregators
1. Add Complex metrics
1. Add new Query types
1. Add new Jersey resources
1. Bundle your extension with all the other Druid extensions

Extensions are added to the system via an implementation of `io.druid.initialization.DruidModule`.

### Creating a Druid Module

The DruidModule class is has two methods

1. A `configure(Binder)` method 
2. A `getJacksonModules()` method

The `configure(Binder)` method is the same method that a normal Guice module would have.

The `getJacksonModules()` method provides a list of Jackson modules that are used to help initialize the Jackson ObjectMapper instances used by Druid.  This is how you add extensions that are instantiated via Jackson (like AggregatorFactory and Firehose objects) to Druid.

### Registering your Druid Module

Once you have your DruidModule created, you will need to package an extra file in the `META-INF/services` directory of your jar.  This is easiest to accomplish with a maven project by creating files in the `src/main/resources` directory.  There are examples of this in the Druid code under the `cassandra-storage`, `hdfs-storage` and `s3-extensions` modules, for examples.

The file that should exist in your jar is

`META-INF/services/io.druid.initialization.DruidModule`

It should be a text file with a new-line delimited list of package-qualified classes that implement DruidModule like

```
io.druid.storage.cassandra.CassandraDruidModule
```

If your jar has this file, then when it is added to the classpath or as an extension, Druid will notice the file and will instantiate instances of the Module.  Your Module should have a default constructor, but if you need access to runtime configuration properties, it can have a method with @Inject on it to get a Properties object injected into it from Guice.

### Adding a new deep storage implementation

Check the `azure-storage`, `google-storage`, `cassandra-storage`, `hdfs-storage` and `s3-extensions` modules for examples of how to do this.

The basic idea behind the extension is that you need to add bindings for your DataSegmentPusher and DataSegmentPuller objects.  The way to add them is something like (taken from HdfsStorageDruidModule)

``` java
Binders.dataSegmentPullerBinder(binder)
       .addBinding("hdfs")
       .to(HdfsDataSegmentPuller.class).in(LazySingleton.class);

Binders.dataSegmentPusherBinder(binder)
       .addBinding("hdfs")
       .to(HdfsDataSegmentPusher.class).in(LazySingleton.class);
```

`Binders.dataSegment*Binder()` is a call provided by the druid-api jar which sets up a Guice multibind "MapBinder".  If that doesn't make sense, don't worry about it, just think of it as a magical incantation.

`addBinding("hdfs")` for the Puller binder creates a new handler for loadSpec objects of type "hdfs".  For the Pusher binder it creates a new type value that you can specify for the `druid.storage.type` parameter.

`to(...).in(...);` is normal Guice stuff.

In addition to DataSegmentPusher and DataSegmentPuller, you can also bind:

* DataSegmentKiller: Removes segments, used as part of the Kill Task to delete unused segments, i.e. perform garbage collection of segments that are either superseded by newer versions or that have been dropped from the cluster. 
* DataSegmentMover: Allow migrating segments from one place to another, currently this is only used as part of the MoveTask to move unused segments to a different S3 bucket or prefix, typically to reduce storage costs of unused data (e.g. move to glacier or cheaper storage) 
* DataSegmentArchiver: Just a wrapper around Mover, but comes with a pre-configured target bucket/path, so it doesn't have to be specified at runtime as part of the ArchiveTask. 

### Validating your deep storage implementation

**WARNING!** This is not a formal procedure, but a collection of hints to validate if your new deep storage implementation is able do push, pull and kill segments.

It's recommended to use batch ingestion tasks to validate your implementation.
The segment will be automatically rolled up to historical note after ~20 seconds. 
In this way, you can validate both push (at realtime node) and pull (at historical node) segments.

* DataSegmentPusher

Wherever your data storage (cloud storage service, distributed file system, etc.) is, you should be able to see two new files: `descriptor.json` (`partitionNum_descriptor.json` for HDFS data storage) and `index.zip` (`partitionNum_index.zip` for HDFS data storage) after your ingestion task ends.

* DataSegmentPuller

After ~20 secs your ingestion task ends, you should be able to see your historical node trying to load the new segment.

The following example was retrieved from a historical node configured to use Azure for deep storage:

```
2015-04-14T02:42:33,450 INFO [ZkCoordinator-0] io.druid.server.coordination.ZkCoordinator - New request[LOAD: dde_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00
.000Z_2015-04-14T02:41:09.484Z] with zNode[/druid/dev/loadQueue/192.168.33.104:8081/dde_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_2015-04-14T02:41:09.
484Z].
2015-04-14T02:42:33,451 INFO [ZkCoordinator-0] io.druid.server.coordination.ZkCoordinator - Loading segment dde_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.0
00Z_2015-04-14T02:41:09.484Z
2015-04-14T02:42:33,463 INFO [ZkCoordinator-0] io.druid.guice.JsonConfigurator - Loaded class[class io.druid.storage.azure.AzureAccountConfig] from props[drui
d.azure.] as [io.druid.storage.azure.AzureAccountConfig@759c9ad9]
2015-04-14T02:49:08,275 INFO [ZkCoordinator-0] com.metamx.common.CompressionUtils - Unzipping file[/opt/druid/tmp/compressionUtilZipCache1263964429587449785.z
ip] to [/opt/druid/zk_druid/dde/2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z/2015-04-14T02:41:09.484Z/0]
2015-04-14T02:49:08,276 INFO [ZkCoordinator-0] io.druid.storage.azure.AzureDataSegmentPuller - Loaded 1196 bytes from [dde/2015-01-02T00:00:00.000Z_2015-01-03
T00:00:00.000Z/2015-04-14T02:41:09.484Z/0/index.zip] to [/opt/druid/zk_druid/dde/2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z/2015-04-14T02:41:09.484Z/0]
2015-04-14T02:49:08,277 WARN [ZkCoordinator-0] io.druid.segment.loading.SegmentLoaderLocalCacheManager - Segment [dde_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_2015-04-14T02:41:09.484Z] is different than expected size. Expected [0] found [1196]
2015-04-14T02:49:08,282 INFO [ZkCoordinator-0] io.druid.server.coordination.BatchDataSegmentAnnouncer - Announcing segment[dde_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_2015-04-14T02:41:09.484Z] at path[/druid/dev/segments/192.168.33.104:8081/192.168.33.104:8081_historical__default_tier_2015-04-14T02:49:08.282Z_7bb87230ebf940188511dd4a53ffd7351]
2015-04-14T02:49:08,292 INFO [ZkCoordinator-0] io.druid.server.coordination.ZkCoordinator - Completed request [LOAD: dde_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_2015-04-14T02:41:09.484Z]
```

* DataSegmentKiller

The easiest way of testing the segment killing is marking a segment as not used and then starting a killing task through the old Coordinator console.

To mark a segment as not used, you need to connect to your metadata storage and update the `used` column to `false` on the segment table rows.

To start a segment killing task, you need to access the old Coordinator console `http://<COODRINATOR_IP>:<COORDINATOR_PORT/old-console/kill.html` then select the appropriate datasource and then input a time range (e.g. `2000/3000`).

After the killing task ends, both `descriptor.json` (`partitionNum_descriptor.json` for HDFS data storage)  and `index.zip` (`partitionNum_index.zip` for HDFS data storage) files should be deleted from the data storage.

### Adding a new Firehose

There is an example of this in the `s3-extensions` module with the StaticS3FirehoseFactory.

Adding a Firehose is done almost entirely through the Jackson Modules instead of Guice.  Specifically, note the implementation

``` java
@Override
public List<? extends Module> getJacksonModules()
{
  return ImmutableList.of(
          new SimpleModule().registerSubtypes(new NamedType(StaticS3FirehoseFactory.class, "static-s3"))
  );
}
```

This is registering the FirehoseFactory with Jackson's polymorphic serde layer.  More concretely, having this will mean that if you specify a `"firehose": { "type": "static-s3", ... }` in your realtime config, then the system will load this FirehoseFactory for your firehose.

Note that inside of Druid, we have made the @JacksonInject annotation for Jackson deserialized objects actually use the base Guice injector to resolve the object to be injected.  So, if your FirehoseFactory needs access to some object, you can add a @JacksonInject annotation on a setter and it will get set on instantiation.

### Adding Aggregators

Adding AggregatorFactory objects is very similar to Firehose objects.  They operate purely through Jackson and thus should just be additions to the Jackson modules returned by your DruidModule.

### Adding Complex Metrics 

Adding ComplexMetrics is a little ugly in the current version.  The method of getting at complex metrics is through registration with the `ComplexMetrics.registerSerde()` method.  There is no special Guice stuff to get this working, just in your `configure(Binder)` method register the serde.

### Adding new Query types

Adding a new Query type requires the implementation of three interfaces.

1. `io.druid.query.Query`
1. `io.druid.query.QueryToolChest`
1. `io.druid.query.QueryRunnerFactory`

Registering these uses the same general strategy as a deep storage mechanism does.  You do something like

``` java
DruidBinders.queryToolChestBinder(binder)
            .addBinding(SegmentMetadataQuery.class)
            .to(SegmentMetadataQueryQueryToolChest.class);
    
DruidBinders.queryRunnerFactoryBinder(binder)
            .addBinding(SegmentMetadataQuery.class)
            .to(SegmentMetadataQueryRunnerFactory.class);
```

The first one binds the SegmentMetadataQueryQueryToolChest for usage when a SegmentMetadataQuery is used.  The second one does the same thing but for the QueryRunnerFactory instead.

### Adding new Jersey resources

Adding new Jersey resources to a module requires calling the following code to bind the resource in the module:

```java
Jerseys.addResource(binder, NewResource.class);
```

### Adding a new Password Provider implementation

You will need to implement `io.druid.metadata.PasswordProvider` interface. For every place where Druid uses PasswordProvider, a new instance of the implementation will be created,
thus make sure all the necessary information required for fetching each password is supplied during object instantiation.
In your implementation of `io.druid.initialization.DruidModule`, `getJacksonModules` should look something like this -

``` java
    return ImmutableList.of(
        new SimpleModule("SomePasswordProviderModule")
            .registerSubtypes(
                new NamedType(SomePasswordProvider.class, "some")
            )
    );
```

where `SomePasswordProvider` is the implementation of `PasswordProvider` interface, you can have a look at `io.druid.metadata.EnvironmentVariablePasswordProvider` for example.

### Bundle your extension with all the other Druid extensions

When you do `mvn install`, Druid extensions will be packaged within the Druid tarball and `extensions` directory, which are both underneath `distribution/target/`.

If you want your extension to be included, you can add your extension's maven coordinate as an argument at
[distribution/pom.xml](https://github.com/druid-io/druid/blob/master/distribution/pom.xml#L95)

During `mvn install`, maven will install your extension to the local maven repository, and then call [pull-deps](../operations/pull-deps.html) to pull your extension from
there. In the end, you should see your extension underneath `distribution/target/extensions` and within Druid tarball.

### Managing dependencies

Managing library collisions can be daunting for extensions which draw in commonly used libraries. Here is a list of group IDs for libraries that are suggested to be specified with a `provided` scope to prevent collision with versions used in druid:
```
"io.druid",
"com.metamx.druid",
"asm",
"org.ow2.asm",
"org.jboss.netty",
"com.google.guava",
"com.google.code.findbugs",
"com.google.protobuf",
"com.esotericsoftware.minlog",
"log4j",
"org.slf4j",
"commons-logging",
"org.eclipse.jetty",
"org.mortbay.jetty",
"com.sun.jersey",
"com.sun.jersey.contribs",
"common-beanutils",
"commons-codec",
"commons-lang",
"commons-cli",
"commons-io",
"javax.activation",
"org.apache.httpcomponents",
"org.apache.zookeeper",
"org.codehaus.jackson",
"com.fasterxml.jackson",
"com.fasterxml.jackson.core",
"com.fasterxml.jackson.dataformat",
"com.fasterxml.jackson.datatype",
"org.roaringbitmap",
"net.java.dev.jets3t"
```
See the documentation in `io.druid.cli.PullDependencies` for more information.
