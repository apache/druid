/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.api.client.util.Lists;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import io.druid.guice.ExtensionsConfig;
import io.druid.guice.GuiceInjectors;
import io.druid.indexer.HadoopDruidDetermineConfigurationJob;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.HadoopDruidIndexerJob;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.indexer.Jobby;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.LockAcquireAction;
import io.druid.indexing.common.actions.LockTryAcquireAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.initialization.Initialization;
import io.druid.timeline.DataSegment;
import io.tesla.aether.internal.DefaultTeslaAether;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;

public class HadoopIndexTask extends AbstractTask
{
  private static final Logger log = new Logger(HadoopIndexTask.class);
  private static final ExtensionsConfig extensionsConfig;

  static {
    extensionsConfig = GuiceInjectors.makeStartupInjector().getInstance(ExtensionsConfig.class);
  }

  private static String getTheDataSource(HadoopIngestionSpec spec, HadoopIngestionSpec config)
  {
    if (spec != null) {
      return spec.getDataSchema().getDataSource();
    }
    return config.getDataSchema().getDataSource();
  }

  @JsonIgnore
  private final HadoopIngestionSpec spec;
  @JsonIgnore
  private final List<String> hadoopDependencyCoordinates;
  @JsonIgnore
  private final String classpathPrefix;

  /**
   * @param spec is used by the HadoopDruidIndexerJob to set up the appropriate parameters
   *             for creating Druid index segments. It may be modified.
   *             
   *             Here, we will ensure that the DbConnectorConfig field of the spec is set to null, such that the
   *             job does not push a list of published segments the database. Instead, we will use the method
   *             IndexGeneratorJob.getPublishedSegments() to simply return a list of the published
   *             segments, and let the indexing service report these segments to the database.
   */

  @JsonCreator
  public HadoopIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("spec") HadoopIngestionSpec spec,
      @JsonProperty("config") HadoopIngestionSpec config, // backwards compat
      @JsonProperty("hadoopCoordinates") String hadoopCoordinates,
      @JsonProperty("hadoopDependencyCoordinates") List<String> hadoopDependencyCoordinates,
      @JsonProperty("classpathPrefix") String classpathPrefix
  )
  {
    super(
        id != null ? id : String.format("index_hadoop_%s_%s", getTheDataSource(spec, config), new DateTime()),
        getTheDataSource(spec, config)
    );


    this.spec = spec == null ? config : spec;

    // Some HadoopIngestionSpec stuff doesn't make sense in the context of the indexing service
    Preconditions.checkArgument(
        this.spec.getIOConfig().getSegmentOutputPath() == null,
        "segmentOutputPath must be absent"
    );
    Preconditions.checkArgument(this.spec.getTuningConfig().getWorkingPath() == null, "workingPath must be absent");
    Preconditions.checkArgument(this.spec.getIOConfig().getMetadataUpdateSpec() == null, "updaterJobSpec must be absent");

    if (hadoopDependencyCoordinates != null) {
      this.hadoopDependencyCoordinates = hadoopDependencyCoordinates;
    } else if (hadoopCoordinates != null) {
      this.hadoopDependencyCoordinates = ImmutableList.of(hadoopCoordinates);
    } else {
      // Will be defaulted to something at runtime, based on taskConfig.
      this.hadoopDependencyCoordinates = null;
    }

    this.classpathPrefix = classpathPrefix;
  }

  @Override
  public String getType()
  {
    return "index_hadoop";
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    Optional<SortedSet<Interval>> intervals = spec.getDataSchema().getGranularitySpec().bucketIntervals();
    if (intervals.isPresent()) {
      Interval interval = JodaUtils.umbrellaInterval(
          JodaUtils.condenseIntervals(
              intervals.get()
          )
      );
      return taskActionClient.submit(new LockTryAcquireAction(interval)).isPresent();
    } else {
      return true;
    }
  }

  @JsonProperty("spec")
  public HadoopIngestionSpec getSpec()
  {
    return spec;
  }

  @JsonProperty
  public List<String> getHadoopDependencyCoordinates()
  {
    return hadoopDependencyCoordinates;
  }

  @JsonProperty
  @Override
  public String getClasspathPrefix()
  {
    return classpathPrefix;
  }

  @SuppressWarnings("unchecked")
  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    final List<String> finalHadoopDependencyCoordinates = hadoopDependencyCoordinates != null
                                                          ? hadoopDependencyCoordinates
                                                          : toolbox.getConfig().getDefaultHadoopCoordinates();

    final DefaultTeslaAether aetherClient = Initialization.getAetherClient(extensionsConfig);

    final List<URL> extensionURLs = Lists.newArrayList();
    for (String coordinate : extensionsConfig.getCoordinates()) {
      final ClassLoader coordinateLoader = Initialization.getClassLoaderForCoordinates(
          aetherClient, coordinate
      );
      extensionURLs.addAll(Arrays.asList(((URLClassLoader) coordinateLoader).getURLs()));
    }

    final List<URL> nonHadoopURLs = Lists.newArrayList();
    nonHadoopURLs.addAll(Arrays.asList(((URLClassLoader) HadoopIndexTask.class.getClassLoader()).getURLs()));

    final List<URL> driverURLs = Lists.newArrayList();
    driverURLs.addAll(nonHadoopURLs);
    // put hadoop dependencies last to avoid jets3t & apache.httpcore version conflicts
    for (String hadoopDependencyCoordinate : finalHadoopDependencyCoordinates) {
      final ClassLoader hadoopLoader = Initialization.getClassLoaderForCoordinates(
          aetherClient, hadoopDependencyCoordinate
      );
      driverURLs.addAll(Arrays.asList(((URLClassLoader) hadoopLoader).getURLs()));
    }

    final URLClassLoader loader = new URLClassLoader(driverURLs.toArray(new URL[driverURLs.size()]), null);
    Thread.currentThread().setContextClassLoader(loader);

    final List<URL> jobUrls = Lists.newArrayList();
    jobUrls.addAll(nonHadoopURLs);
    jobUrls.addAll(extensionURLs);

    System.setProperty("druid.hadoop.internal.classpath", Joiner.on(File.pathSeparator).join(jobUrls));
    boolean determineIntervals = !spec.getDataSchema().getGranularitySpec().bucketIntervals().isPresent();

    final Class<?> determineConfigurationMainClass = loader.loadClass(HadoopDetermineConfigInnerProcessing.class.getName());
    final Method determineConfigurationMainMethod = determineConfigurationMainClass.getMethod(
        "runTask",
        String[].class
    );

    String[] determineConfigArgs = new String[]{
        toolbox.getObjectMapper().writeValueAsString(spec),
        toolbox.getConfig().getHadoopWorkingPath(),
        toolbox.getSegmentPusher().getPathForHadoop(getDataSource())
    };

    String config = (String) determineConfigurationMainMethod.invoke(null, new Object[]{determineConfigArgs});
    HadoopIngestionSpec indexerSchema = toolbox.getObjectMapper()
                                               .readValue(config, HadoopIngestionSpec.class);


    // We should have a lock from before we started running only if interval was specified
    final String version;
    if (determineIntervals) {
      Interval interval = JodaUtils.umbrellaInterval(
          JodaUtils.condenseIntervals(
              indexerSchema.getDataSchema().getGranularitySpec().bucketIntervals().get()
          )
      );
      TaskLock lock = toolbox.getTaskActionClient().submit(new LockAcquireAction(interval));
      version = lock.getVersion();
    } else {
      Iterable<TaskLock> locks = getTaskLocks(toolbox);
      final TaskLock myLock = Iterables.getOnlyElement(locks);
      version = myLock.getVersion();
    }
    log.info("Setting version to: %s", version);

    final Class<?> indexGeneratorMainClass = loader.loadClass(HadoopIndexGeneratorInnerProcessing.class.getName());
    final Method indexGeneratorMainMethod = indexGeneratorMainClass.getMethod("runTask", String[].class);
    String[] indexGeneratorArgs = new String[]{
        toolbox.getObjectMapper().writeValueAsString(indexerSchema),
        version
    };
    String segments = (String) indexGeneratorMainMethod.invoke(null, new Object[]{indexGeneratorArgs});


    if (segments != null) {

      List<DataSegment> publishedSegments = toolbox.getObjectMapper().readValue(
          segments,
          new TypeReference<List<DataSegment>>()
          {
          }
      );

      toolbox.pushSegments(publishedSegments);
      return TaskStatus.success(getId());
    } else {
      return TaskStatus.failure(getId());
    }
  }

  public static class HadoopIndexGeneratorInnerProcessing
  {
    public static String runTask(String[] args) throws Exception
    {
      final String schema = args[0];
      String version = args[1];

      final HadoopIngestionSpec theSchema = HadoopDruidIndexerConfig.jsonMapper
          .readValue(
              schema,
              HadoopIngestionSpec.class
          );
      final HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromSpec(
          theSchema
              .withTuningConfig(theSchema.getTuningConfig().withVersion(version))
      );

      HadoopDruidIndexerJob job = new HadoopDruidIndexerJob(config);

      log.info("Starting a hadoop index generator job...");
      if (job.run()) {
        return HadoopDruidIndexerConfig.jsonMapper.writeValueAsString(job.getPublishedSegments());
      }

      return null;
    }
  }

  public static class HadoopDetermineConfigInnerProcessing
  {
    public static String runTask(String[] args) throws Exception
    {
      final String schema = args[0];
      final String workingPath = args[1];
      final String segmentOutputPath = args[2];

      final HadoopIngestionSpec theSchema = HadoopDruidIndexerConfig.jsonMapper
          .readValue(
              schema,
              HadoopIngestionSpec.class
          );
      final HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromSpec(
          theSchema
              .withIOConfig(theSchema.getIOConfig().withSegmentOutputPath(segmentOutputPath))
              .withTuningConfig(theSchema.getTuningConfig().withWorkingPath(workingPath))
      );

      Jobby job = new HadoopDruidDetermineConfigurationJob(config);

      log.info("Starting a hadoop determine configuration job...");
      if (job.run()) {
        return HadoopDruidIndexerConfig.jsonMapper.writeValueAsString(config.getSpec());
      }

      return null;
    }
  }
}
