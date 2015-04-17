---
layout: doc_page
---

# Extending Druid With Custom Modules

Druid uses a module system that allows for the addition of extensions at runtime.

## Specifying extensions

There are two ways of adding druid extensions currently.

### Add to the classpath

If you add your extension jar to the classpath at runtime, Druid will load it into the system.  This mechanism is relatively easy to reason about, but it also means that you have to ensure that all dependency jars on the classpath are compatible.  That is, Druid makes no provisions while using this method to maintain class loader isolation so you must make sure that the jars on your classpath are mutually compatible.

### Specify maven coordinates

Druid has the ability to automatically load extension jars from maven at runtime.  With this mechanism, Druid also loads up the dependencies of the extension jar into an isolated class loader.  That means that your extension can depend on a different version of a library that Druid also uses and both can co-exist.

## Configuring the extensions

Druid provides the following settings to configure the loading of extensions:

* `druid.extensions.coordinates`
    This is a JSON array of "groupId:artifactId[:version]" maven coordinates. For artifacts without version specified, Druid will append the default version. Defaults to `[]`
* `druid.extensions.defaultVersion`
    Version to use for extension artifacts without version information. Defaults to the `druid-server` artifact version.
* `druid.extensions.localRepository`
    This specifies where to look for the "local repository".  The way maven gets dependencies is that it downloads them to a "local repository" on your local disk and then collects the paths to each of the jars.  This specifies the directory to consider the "local repository".  Defaults to `~/.m2/repository`
* `druid.extensions.remoteRepositories`
    This is a JSON Array list of remote repositories to load dependencies from.  Defaults to `["http://repo1.maven.org/maven2/", "https://metamx.artifactoryonline.com/metamx/pub-libs-releases-local"]`
* `druid.extensions.searchCurrentClassloader`
    This is a boolean flag that determines if Druid will search the main classloader for extensions.  It defaults to true but can be turned off if you have reason to not automatically add all modules on the classpath.

### I want classloader isolation, but I don't want my production machines downloading their own dependencies.  What should I do?

If you want to take advantage of the maven-based classloader isolation but you are also rightly frightened by the prospect of each of your production machines downloading their own dependencies on deploy, this section is for you.

The trick to doing this is

1) Specify a local directory for `druid.extensions.localRepository`

2) Run the `tools pull-deps` command to pull all the specified dependencies down into your local repository

3) Bundle up the local repository along with your other Druid stuff into whatever you use for a deployable artifact

4) Run Your druid processes with `druid.extensions.remoteRepositories=[]` and a local repository set to wherever your bundled "local" repository is located

The Druid processes will then only load up jars from the local repository and will not try to go out onto the internet to find the maven dependencies.

## Writing your own extensions

Druid's extensions leverage Guice in order to add things at runtime.  Basically, Guice is a framework for Dependency Injection, but we use it to hold the expected object graph of the Druid process.  Extensions can make any changes they want/need to the object graph via adding Guice bindings.  While the extensions actually give you the capability to change almost anything however you want, in general, we expect people to want to extend one of a few things.

1. Add a new deep storage implementation
1. Add a new Firehose
1. Add Aggregators
1. Add Complex metrics
1. Add new Query types
1. Add new Jersey resources


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

Check the `azure-storage`, `cassandra-storage`, `hdfs-storage` and `s3-extensions` modules for examples of how to do this.

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
* DataSegmentMover: Allow migrating segments from one place to another. Currently this is only used as part of the MoveTask to move unused segments to a different S3 bucket or prefix in order to reduce storage costs of unused data (e.g. move to glacier or cheaper storage).
* DataSegmentArchiver: It's a wrapper around Mover which comes with a pre-configured target bucket/path. Thus it's not necessary to specify those parameters at runtime as part of the ArchiveTask.

### Validating your deep storage implementation

**WARNING!** This is not a formal procedure, but a collection of hints to validate if your new deep storage implementation is able do push, pull and kill segments.

It's recommended to use batch ingestion tasks to validate your implementation.
The segment will be automatically rolled up to historical note after ~20 seconds. 
In this way, you can validate both push (at realtime node) and pull (at historical node) segments.

* DataSegmentPusher

Wherever your data storage (cloud storage service, distributed file system, etc.) is, you should be able to see two new files: `descriptor.json` and `index.zip` after your ingestion task ends.

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

After the killing task ends, both `descriptor.json` and `index.zip` files should be deleted from the data storage.

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
