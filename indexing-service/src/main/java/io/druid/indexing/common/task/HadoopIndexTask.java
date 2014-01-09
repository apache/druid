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
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.HadoopDruidIndexerConfigBuilder;
import io.druid.indexer.HadoopDruidIndexerJob;
import io.druid.indexer.HadoopDruidIndexerSchema;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.LockTryAcquireAction;
import io.druid.indexing.common.actions.SegmentInsertAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.initialization.Initialization;
import io.druid.server.initialization.ExtensionsConfig;
import io.druid.timeline.DataSegment;
import io.tesla.aether.internal.DefaultTeslaAether;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class HadoopIndexTask extends AbstractFixedIntervalTask
{
  private static final Logger log = new Logger(HadoopIndexTask.class);
  private static String defaultHadoopCoordinates = "org.apache.hadoop:hadoop-core:1.0.3";

  private static final ExtensionsConfig extensionsConfig;

  static {
    extensionsConfig = Initialization.makeStartupInjector().getInstance(ExtensionsConfig.class);
  }

  @JsonIgnore
  private final HadoopDruidIndexerSchema schema;

  @JsonIgnore
  private final String hadoopCoordinates;

  /**
   * @param schema is used by the HadoopDruidIndexerJob to set up the appropriate parameters
   *               for creating Druid index segments. It may be modified.
   *               <p/>
   *               Here, we will ensure that the DbConnectorConfig field of the schema is set to null, such that the
   *               job does not push a list of published segments the database. Instead, we will use the method
   *               IndexGeneratorJob.getPublishedSegments() to simply return a list of the published
   *               segments, and let the indexing service report these segments to the database.
   */

  @JsonCreator
  public HadoopIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("config") HadoopDruidIndexerSchema schema,
      @JsonProperty("hadoopCoordinates") String hadoopCoordinates
  )
  {
    super(
        id != null ? id : String.format("index_hadoop_%s_%s", schema.getDataSource(), new DateTime()),
        schema.getDataSource(),
        JodaUtils.umbrellaInterval(
            JodaUtils.condenseIntervals(
                schema.getGranularitySpec()
                      .bucketIntervals()
            )
        )
    );

    // Some HadoopDruidIndexerSchema stuff doesn't make sense in the context of the indexing service
    Preconditions.checkArgument(schema.getSegmentOutputPath() == null, "segmentOutputPath must be absent");
    Preconditions.checkArgument(schema.getWorkingPath() == null, "workingPath must be absent");
    Preconditions.checkArgument(schema.getUpdaterJobSpec() == null, "updaterJobSpec must be absent");

    this.schema = schema;
    this.hadoopCoordinates = (hadoopCoordinates == null ? defaultHadoopCoordinates : hadoopCoordinates);
  }

  @Override
  public String getType()
  {
    return "index_hadoop";
  }

  @JsonProperty("config")
  public HadoopDruidIndexerSchema getSchema()
  {
    return schema;
  }

  @JsonProperty
  public String getHadoopCoordinates()
  {
    return hadoopCoordinates;
  }

  @SuppressWarnings("unchecked")
  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    // setup Hadoop
    final DefaultTeslaAether aetherClient = Initialization.getAetherClient(extensionsConfig);
    final ClassLoader hadoopLoader = Initialization.getClassLoaderForCoordinates(
        aetherClient, hadoopCoordinates
    );

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
    driverURLs.addAll(Arrays.asList(((URLClassLoader) hadoopLoader).getURLs()));

    final URLClassLoader loader = new URLClassLoader(driverURLs.toArray(new URL[driverURLs.size()]), null);
    Thread.currentThread().setContextClassLoader(loader);

    final List<URL> jobUrls = Lists.newArrayList();
    jobUrls.addAll(nonHadoopURLs);
    jobUrls.addAll(extensionURLs);

    System.setProperty("druid.hadoop.internal.classpath", Joiner.on(File.pathSeparator).join(jobUrls));

    final Class<?> mainClass = loader.loadClass(HadoopIndexTaskInnerProcessing.class.getName());
    final Method mainMethod = mainClass.getMethod("runTask", String[].class);

    // We should have a lock from before we started running
    final TaskLock myLock = Iterables.getOnlyElement(getTaskLocks(toolbox));
    log.info("Setting version to: %s", myLock.getVersion());

    String[] args = new String[]{
        toolbox.getObjectMapper().writeValueAsString(schema),
        myLock.getVersion(),
        toolbox.getConfig().getHadoopWorkingPath(),
        toolbox.getSegmentPusher().getPathForHadoop(getDataSource()),
    };

    String segments = (String) mainMethod.invoke(null, new Object[]{args});


    if (segments != null) {
      List<DataSegment> publishedSegments = toolbox.getObjectMapper().readValue(
          segments,
          new TypeReference<List<DataSegment>>() {}
      );
      toolbox.pushSegments(publishedSegments);
      return TaskStatus.success(getId());
    } else {
      return TaskStatus.failure(getId());
    }
  }

  public static class HadoopIndexTaskInnerProcessing
  {
    public static String runTask(String[] args) throws Exception
    {
      final String schema = args[0];
      final String version = args[1];
      final String workingPath = args[2];
      final String segmentOutputPath = args[3];

      final HadoopDruidIndexerSchema theSchema = HadoopDruidIndexerConfig.jsonMapper
                                                                         .readValue(
                                                                             schema,
                                                                             HadoopDruidIndexerSchema.class
                                                                         );
      final HadoopDruidIndexerConfig config =
          new HadoopDruidIndexerConfigBuilder().withSchema(theSchema)
                                               .withVersion(version)
                                               .withWorkingPath(
                                                   workingPath
                                               )
                                               .withSegmentOutputPath(
                                                   segmentOutputPath
                                               )
                                               .build();

      HadoopDruidIndexerJob job = new HadoopDruidIndexerJob(config);

      log.info("Starting a hadoop index generator job...");
      if (job.run()) {
        return HadoopDruidIndexerConfig.jsonMapper.writeValueAsString(job.getPublishedSegments());
      }

      return null;
    }
  }
}
