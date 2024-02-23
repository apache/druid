---
id: modules
title: "Creating extensions"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->


Druid uses a module system that allows for the addition of extensions at runtime.

## Writing your own extensions

Druid's extensions leverage Guice in order to add things at runtime.  Basically, Guice is a framework for Dependency Injection, but we use it to hold the expected object graph of the Druid process.  Extensions can make any changes they want/need to the object graph via adding Guice bindings.  While the extensions actually give you the capability to change almost anything however you want, in general, we expect people to want to extend one of the things listed below.  This means that we honor our [versioning strategy](./versioning.md) for changes that affect the interfaces called out on this page, but other interfaces are deemed "internal" and can be changed in an incompatible manner even between patch releases.

1. Add a new deep storage implementation by extending the `org.apache.druid.segment.loading.DataSegment*` and
   `org.apache.druid.tasklogs.TaskLog*` classes.
1. Add a new input source by extending `org.apache.druid.data.input.InputSource`.
1. Add a new input entity by extending `org.apache.druid.data.input.InputEntity`.
1. Add a new input source reader if necessary by extending `org.apache.druid.data.input.InputSourceReader`. You can use `org.apache.druid.data.input.impl.InputEntityIteratingReader` in most cases.
1. Add a new input format by extending `org.apache.druid.data.input.InputFormat`.
1. Add a new input entity reader by extending `org.apache.druid.data.input.TextReader` for text formats or `org.apache.druid.data.input.IntermediateRowParsingReader` for binary formats.
1. Add Aggregators by extending `org.apache.druid.query.aggregation.AggregatorFactory`, `org.apache.druid.query.aggregation.Aggregator`,
   and `org.apache.druid.query.aggregation.BufferAggregator`.
1. Add PostAggregators by extending `org.apache.druid.query.aggregation.PostAggregator`.
1. Add ExtractionFns by extending `org.apache.druid.query.extraction.ExtractionFn`.
1. Add Complex metrics by extending `org.apache.druid.segment.serde.ComplexMetricSerde`.
1. Add new Query types by extending `org.apache.druid.query.QueryRunnerFactory`, `org.apache.druid.query.QueryToolChest`, and
   `org.apache.druid.query.Query`.
1. Add new Jersey resources by calling `Jerseys.addResource(binder, clazz)`.
1. Add new Jetty filters by extending `org.apache.druid.server.initialization.jetty.ServletFilterHolder`.
1. Add new secret providers by extending `org.apache.druid.metadata.PasswordProvider`.
1. Add new dynamic configuration providers by extending `org.apache.druid.metadata.DynamicConfigProvider`.
1. Add new ingest transform by implementing the `org.apache.druid.segment.transform.Transform` interface from the `druid-processing` package.
1. Bundle your extension with all the other Druid extensions

Extensions are added to the system via an implementation of `org.apache.druid.initialization.DruidModule`.

### Creating a Druid Module

The DruidModule class is has two methods

1. A `configure(Binder)` method
2. A `getJacksonModules()` method

The `configure(Binder)` method is the same method that a normal Guice module would have.

The `getJacksonModules()` method provides a list of Jackson modules that are used to help initialize the Jackson ObjectMapper instances used by Druid.  This is how you add extensions that are instantiated via Jackson (like AggregatorFactory and InputSource objects) to Druid.

### Registering your Druid Module

Once you have your DruidModule created, you will need to package an extra file in the `META-INF/services` directory of your jar.  This is easiest to accomplish with a maven project by creating files in the `src/main/resources` directory.  There are examples of this in the Druid code under the `cassandra-storage`, `hdfs-storage` and `s3-extensions` modules, for examples.

The file that should exist in your jar is

`META-INF/services/org.apache.druid.initialization.DruidModule`

It should be a text file with a new-line delimited list of package-qualified classes that implement DruidModule like

```
org.apache.druid.storage.cassandra.CassandraDruidModule
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

`Binders.dataSegment*Binder()` is a call provided by the druid-core jar which sets up a Guice multibind "MapBinder".  If that doesn't make sense, don't worry about it, just think of it as a magical incantation.

`addBinding("hdfs")` for the Puller binder creates a new handler for loadSpec objects of type "hdfs".  For the Pusher binder it creates a new type value that you can specify for the `druid.storage.type` parameter.

`to(...).in(...);` is normal Guice stuff.

In addition to DataSegmentPusher and DataSegmentPuller, you can also bind:

* DataSegmentKiller: Removes segments, used as part of the Kill Task to delete unused segments, i.e. perform garbage collection of segments that are either superseded by newer versions or that have been dropped from the cluster.
* DataSegmentMover: Allow migrating segments from one place to another, currently this is only used as part of the MoveTask to move unused segments to a different S3 bucket or prefix, typically to reduce storage costs of unused data (e.g. move to glacier or cheaper storage)
* DataSegmentArchiver: Just a wrapper around Mover, but comes with a preconfigured target bucket/path, so it doesn't have to be specified at runtime as part of the ArchiveTask.

### Validating your deep storage implementation

**WARNING!** This is not a formal procedure, but a collection of hints to validate if your new deep storage implementation is able do push, pull and kill segments.

It's recommended to use batch ingestion tasks to validate your implementation.
The segment will be automatically rolled up to Historical note after ~20 seconds.
In this way, you can validate both push (at realtime process) and pull (at Historical process) segments.

* DataSegmentPusher

Wherever your data storage (cloud storage service, distributed file system, etc.) is, you should be able to see one new file: `index.zip` (`partitionNum_index.zip` for HDFS data storage) after your ingestion task ends.

* DataSegmentPuller

After ~20 secs your ingestion task ends, you should be able to see your Historical process trying to load the new segment.

The following example was retrieved from a Historical process configured to use Azure for deep storage:

```
2015-04-14T02:42:33,450 INFO [ZkCoordinator-0] org.apache.druid.server.coordination.ZkCoordinator - New request[LOAD: dde_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00
.000Z_2015-04-14T02:41:09.484Z] with zNode[/druid/dev/loadQueue/192.168.33.104:8081/dde_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_2015-04-14T02:41:09.
484Z].
2015-04-14T02:42:33,451 INFO [ZkCoordinator-0] org.apache.druid.server.coordination.ZkCoordinator - Loading segment dde_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.0
00Z_2015-04-14T02:41:09.484Z
2015-04-14T02:42:33,463 INFO [ZkCoordinator-0] org.apache.druid.guice.JsonConfigurator - Loaded class[class org.apache.druid.storage.azure.AzureAccountConfig] from props[drui
d.azure.] as [org.apache.druid.storage.azure.AzureAccountConfig@759c9ad9]
2015-04-14T02:49:08,275 INFO [ZkCoordinator-0] org.apache.druid.utils.CompressionUtils - Unzipping file[/opt/druid/tmp/compressionUtilZipCache1263964429587449785.z
ip] to [/opt/druid/zk_druid/dde/2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z/2015-04-14T02:41:09.484Z/0]
2015-04-14T02:49:08,276 INFO [ZkCoordinator-0] org.apache.druid.storage.azure.AzureDataSegmentPuller - Loaded 1196 bytes from [dde/2015-01-02T00:00:00.000Z_2015-01-03
T00:00:00.000Z/2015-04-14T02:41:09.484Z/0/index.zip] to [/opt/druid/zk_druid/dde/2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z/2015-04-14T02:41:09.484Z/0]
2015-04-14T02:49:08,277 WARN [ZkCoordinator-0] org.apache.druid.segment.loading.SegmentLocalCacheManager - Segment [dde_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_2015-04-14T02:41:09.484Z] is different than expected size. Expected [0] found [1196]
2015-04-14T02:49:08,282 INFO [ZkCoordinator-0] org.apache.druid.server.coordination.BatchDataSegmentAnnouncer - Announcing segment[dde_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_2015-04-14T02:41:09.484Z] at path[/druid/dev/segments/192.168.33.104:8081/192.168.33.104:8081_historical__default_tier_2015-04-14T02:49:08.282Z_7bb87230ebf940188511dd4a53ffd7351]
2015-04-14T02:49:08,292 INFO [ZkCoordinator-0] org.apache.druid.server.coordination.ZkCoordinator - Completed request [LOAD: dde_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_2015-04-14T02:41:09.484Z]
```

* DataSegmentKiller

The easiest way of testing the segment killing is marking a segment as not used and then starting a killing task in the [web console](../operations/web-console.md).

To mark a segment as not used, you need to connect to your metadata storage and update the `used` column to `false` on the segment table rows.

To start a segment killing task, you need to access the web console then select `issue kill task` for the appropriate datasource.

After the killing task ends, `index.zip` (`partitionNum_index.zip` for HDFS data storage) file should be deleted from the data storage.

### Adding support for a new input source

Adding support for a new input source requires to implement three interfaces, i.e., `InputSource`, `InputEntity`, and `InputSourceReader`.
`InputSource` is to define where the input data is stored. `InputEntity` is to define how data can be read in parallel
in [native parallel indexing](../ingestion/native-batch.md).
`InputSourceReader` defines how to read your new input source and you can simply use the provided `InputEntityIteratingReader` in most cases.

There is an example of this in the `druid-s3-extensions` module with the `S3InputSource` and `S3Entity`.

Adding an InputSource is done almost entirely through the Jackson Modules instead of Guice. Specifically, note the implementation

``` java
@Override
public List<? extends Module> getJacksonModules()
{
  return ImmutableList.of(
          new SimpleModule().registerSubtypes(new NamedType(S3InputSource.class, "s3"))
  );
}
```

This is registering the InputSource with Jackson's polymorphic serialization/deserialization layer.  More concretely, having this will mean that if you specify a `"inputSource": { "type": "s3", ... }` in your IO config, then the system will load this InputSource for your `InputSource` implementation.

Note that inside of Druid, we have made the `@JacksonInject` annotation for Jackson deserialized objects actually use the base Guice injector to resolve the object to be injected.  So, if your InputSource needs access to some object, you can add a `@JacksonInject` annotation on a setter and it will get set on instantiation.

### Adding support for a new data format

Adding support for a new data format requires implementing two interfaces, i.e., `InputFormat` and `InputEntityReader`.
`InputFormat` is to define how your data is formatted. `InputEntityReader` is to define how to parse your data and convert into Druid `InputRow`.

There is an example in the `druid-orc-extensions` module with the `OrcInputFormat` and `OrcReader`.
 
Adding an InputFormat is very similar to adding an InputSource. They operate purely through Jackson and thus should just be additions to the Jackson modules returned by your DruidModule.

### Adding Aggregators

Adding AggregatorFactory objects is very similar to InputSource objects.  They operate purely through Jackson and thus should just be additions to the Jackson modules returned by your DruidModule.

### Adding Complex Metrics

Adding ComplexMetrics is a little ugly in the current version.  The method of getting at complex metrics is through registration with the `ComplexMetrics.registerSerde()` method.  There is no special Guice stuff to get this working, just in your `configure(Binder)` method register the serialization/deserialization.

### Adding new Query types

Adding a new Query type requires the implementation of three interfaces.

1. `org.apache.druid.query.Query`
1. `org.apache.druid.query.QueryToolChest`
1. `org.apache.druid.query.QueryRunnerFactory`

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

You will need to implement `org.apache.druid.metadata.PasswordProvider` interface. For every place where Druid uses PasswordProvider, a new instance of the implementation will be created,
thus make sure all the necessary information required for fetching each password is supplied during object instantiation.
In your implementation of `org.apache.druid.initialization.DruidModule`, `getJacksonModules` should look something like this -

``` java
    return ImmutableList.of(
        new SimpleModule("SomePasswordProviderModule")
            .registerSubtypes(
                new NamedType(SomePasswordProvider.class, "some")
            )
    );
```

where `SomePasswordProvider` is the implementation of `PasswordProvider` interface, you can have a look at `org.apache.druid.metadata.EnvironmentVariablePasswordProvider` for example.

### Adding a new DynamicConfigProvider implementation

You will need to implement `org.apache.druid.metadata.DynamicConfigProvider` interface. For every place where Druid uses DynamicConfigProvider, a new instance of the implementation will be created,
thus make sure all the necessary information required for fetching all information is supplied during object instantiation.
In your implementation of `org.apache.druid.initialization.DruidModule`, `getJacksonModules` should look something like this -

``` java
    return ImmutableList.of(
        new SimpleModule("SomeDynamicConfigProviderModule")
            .registerSubtypes(
                new NamedType(SomeDynamicConfigProvider.class, "some")
            )
    );
```

where `SomeDynamicConfigProvider` is the implementation of `DynamicConfigProvider` interface, you can have a look at `org.apache.druid.metadata.MapStringDynamicConfigProvider` for example.

### Adding a Transform Extension

To create a transform extension implement the `org.apache.druid.segment.transform.Transform` interface. You'll need to install the `druid-processing` package to import `org.apache.druid.segment.transform`.

```java
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.transform.RowFunction;
import org.apache.druid.segment.transform.Transform;

public class MyTransform implements Transform {
    private final String name;

    @JsonCreator
    public MyTransform(
        @JsonProperty("name") final String name
    ) {
        this.name = name;
    }

    @JsonProperty
    @Override
    public String getName() {
        return name;
    }

    @Override
    public RowFunction getRowFunction() {
        return new MyRowFunction();
    }

    static class MyRowFunction implements RowFunction {
        @Override
        public Object eval(Row row) {
            return "transformed-value";
        }
    }
}
```

Then register your transform as a Jackson module.

```java
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedModule;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.common.collect.ImmutableList;
import org.apache.druid.initialization.DruidModule;

public class MyTransformModule implements DruidModule {
    @Override
    public List<? extends Module> getJacksonModules() {
        return return ImmutableList.of(
            new SimpleModule("MyTransformModule").registerSubtypes(
                new NamedType(MyTransform.class, "my-transform")
            )
        ):
    }

    @Override
    public void configure(Binder binder) {
    }
}
```

### Adding your own custom pluggable Coordinator Duty

The coordinator periodically runs jobs, so-called `CoordinatorDuty` which include loading new segments, segment balancing, etc. 
Druid users can add custom pluggable coordinator duties, which are not part of Core Druid, without modifying any Core Druid classes.
Users can do this by writing their own custom coordinator duty implementing the interface `CoordinatorCustomDuty` and setting the `JsonTypeName`.
Next, users will need to register their custom coordinator as subtypes in their Module's `DruidModule#getJacksonModules()`.
Once these steps are done, user will be able to load their custom coordinator duty using the following properties:
```
druid.coordinator.dutyGroups=[<GROUP_NAME_1>, <GROUP_NAME_2>, ...]
druid.coordinator.<GROUP_NAME_1>.duties=[<DUTY_NAME_MATCHING_JSON_TYPE_NAME_1>, <DUTY_NAME_MATCHING_JSON_TYPE_NAME_2>, ...]
druid.coordinator.<GROUP_NAME_1>.period=<GROUP_NAME_1_RUN_PERIOD>

druid.coordinator.<GROUP_NAME_1>.duty.<DUTY_NAME_MATCHING_JSON_TYPE_NAME_1>.<SOME_CONFIG_1_KEY>=<SOME_CONFIG_1_VALUE>
druid.coordinator.<GROUP_NAME_1>.duty.<DUTY_NAME_MATCHING_JSON_TYPE_NAME_1>.<SOME_CONFIG_2_KEY>=<SOME_CONFIG_2_VALUE>
```
In the new system for pluggable Coordinator duties, similar to what coordinator already does today, the duties can be grouped together.
The duties will be grouped into multiple groups as per the elements in list `druid.coordinator.dutyGroups`. 
All duties in the same group will have the same run period configured by `druid.coordinator.<GROUP_NAME>.period`.
Currently, there is a single thread running the duties sequentially for each group. 

For example, see `KillSupervisorsCustomDuty` for a custom coordinator duty implementation and the `custom-coordinator-duties`
integration test group which loads `KillSupervisorsCustomDuty` using the configs set in `integration-tests/docker/environment-configs/test-groups/custom-coordinator-duties`.
This config file adds the configs below to enable a custom coordinator duty.

```
druid.coordinator.dutyGroups=["cleanupMetadata"]
druid.coordinator.cleanupMetadata.duties=["killSupervisors"]
druid.coordinator.cleanupMetadata.duty.killSupervisors.durationToRetain=PT0M
druid.coordinator.cleanupMetadata.period=PT10S
```

These configurations create a custom coordinator duty group called `cleanupMetadata` which runs a custom coordinator duty called `killSupervisors` every 10 seconds.
The custom coordinator duty `killSupervisors` also has a config called `durationToRetain` which is set to 0 minute.

### Routing data through a HTTP proxy for your extension

You can add the ability for the `HttpClient` of your extension to connect through an HTTP proxy. 

To support proxy connection for your extension's HTTP client:
1. Add `HttpClientProxyConfig` as a `@JsonProperty` to the HTTP config class of your extension. 
2. In the extension's module class, add `HttpProxyConfig` config to `HttpClientConfig`. 
For example, where `config` variable is the extension's HTTP config from step 1:
```
final HttpClientConfig.Builder builder = HttpClientConfig
    .builder()
    .withNumConnections(1)
    .withReadTimeout(config.getReadTimeout().toStandardDuration())
    .withHttpProxyConfig(config.getProxyConfig());
```

### Bundle your extension with all the other Druid extensions

When you do `mvn install`, Druid extensions will be packaged within the Druid tarball and `extensions` directory, which are both underneath `distribution/target/`.

If you want your extension to be included, you can add your extension's maven coordinate as an argument at
[distribution/pom.xml](https://github.com/apache/druid/blob/master/distribution/pom.xml#L95)

During `mvn install`, maven will install your extension to the local maven repository, and then call [pull-deps](../operations/pull-deps.md) to pull your extension from
there. In the end, you should see your extension underneath `distribution/target/extensions` and within Druid tarball.

### Managing dependencies

Managing library collisions can be daunting for extensions which draw in commonly used libraries. Here is a list of group IDs for libraries that are suggested to be specified with a `provided` scope to prevent collision with versions used in druid:
```
"org.apache.druid",
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
See the documentation in `org.apache.druid.cli.PullDependencies` for more information.
