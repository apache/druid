/*
 *
 *  Licensed to Metamarkets Group Inc. (Metamarkets) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. Metamarkets licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 * /
 *
 */

package io.druid.hadoopMultiIndexTaskExt;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import io.druid.indexer.HadoopDruidDetermineConfigurationJob;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.HadoopDruidIndexerJob;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.indexer.Jobby;
import io.druid.indexer.MetadataStorageUpdaterJobHandler;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.DataSourcesLockAcquireAction;
import io.druid.indexing.common.actions.DataSourcesLockTryAcquireAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.HadoopTask;
import io.druid.indexing.hadoop.OverlordActionBasedUsedSegmentLister;
import io.druid.indexing.overlord.DataSourceAndInterval;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class HadoopMultiIndexTask extends HadoopTask
{
  private static final Logger log = new Logger(HadoopMultiIndexTask.class);

  private static List<String> getTheDataSources(HadoopIngestionSpec[] specs)
  {
    List<String> dataSources = new ArrayList<>(specs.length);

    for(HadoopIngestionSpec spec : specs) {
      dataSources.add(spec.getDataSchema().getDataSource());
    }

    return dataSources;
  }

  @JsonIgnore
  private HadoopIngestionSpec[] specs;

  @JsonIgnore
  private final String classpathPrefix;

  @JsonIgnore
  private final ObjectMapper jsonMapper;

  /**
   * Very similar to HadoopIndexTask except that it creates multiple dataSources. Same can be achieved
   * by running HadoopIndexTask multiple times for each DataSource but this implementation publishes
   * segments for all DataSources in one transaction.
   */

  @JsonCreator
  public HadoopMultiIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("specs") HadoopIngestionSpec[] specs,
      @JsonProperty("hadoopCoordinates") String hadoopCoordinates,
      @JsonProperty("hadoopDependencyCoordinates") List<String> hadoopDependencyCoordinates,
      @JsonProperty("classpathPrefix") String classpathPrefix,
      @JacksonInject ObjectMapper jsonMapper,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        id != null
        ? id
        : String.format(
            "multi_index_hadoop_%s--%s_%s",
            specs[0].getDataSchema().getDataSource(),
            specs[specs.length - 1].getDataSchema().getDataSource(),
            new DateTime()
        ),
        getTheDataSources(specs),
        hadoopDependencyCoordinates == null
        ? (hadoopCoordinates == null ? null : ImmutableList.of(hadoopCoordinates))
        : hadoopDependencyCoordinates,
        context
    );


    this.specs = specs;

    // Some HadoopIngestionSpec stuff doesn't make sense in the context of the indexing service
    for (HadoopIngestionSpec spec : specs) {
      Preconditions.checkArgument(
          spec.getIOConfig().getSegmentOutputPath() == null,
          "segmentOutputPath must be absent"
      );
      Preconditions.checkArgument(spec.getTuningConfig().getWorkingPath() == null, "workingPath must be absent");
      Preconditions.checkArgument(
          spec.getIOConfig().getMetadataUpdateSpec() == null,
          "metadataUpdateSpec must be absent"
      );
    }

    this.classpathPrefix = classpathPrefix;
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "null ObjectMappper");
  }

  @Override
  public String getType()
  {
    return "multi_index_hadoop";
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    List<DataSourceAndInterval> dataSourceAndIntervals = new ArrayList<>(specs.length);

    for (HadoopIngestionSpec spec : specs) {
      Optional<SortedSet<Interval>> intervals = spec.getDataSchema().getGranularitySpec().bucketIntervals();
      if (intervals.isPresent()) {
        Interval interval = JodaUtils.umbrellaInterval(
            JodaUtils.condenseIntervals(
                intervals.get()
            )
        );
        dataSourceAndIntervals.add(new DataSourceAndInterval(spec.getDataSchema().getDataSource(), interval));
      }
    }

    if (dataSourceAndIntervals.size() > 0) {
      return taskActionClient.submit(new DataSourcesLockTryAcquireAction(dataSourceAndIntervals)) != null;
    }

    return true;
  }

  @JsonProperty("specs")
  public HadoopIngestionSpec[] getSpec()
  {
    return specs;
  }

  @JsonProperty
  public List<String> getHadoopDependencyCoordinates()
  {
    return super.getHadoopDependencyCoordinates();
  }

  @JsonProperty
  @Override
  public String getClasspathPrefix()
  {
    return classpathPrefix;
  }

  @SuppressWarnings("unchecked")
  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    log.info("Starting Task to create following dataSources - %s", getDataSources());

    final ClassLoader loader = buildClassLoader(toolbox);

    final ExecutorService executor = Executors.newFixedThreadPool(specs.length);

    Map<String, Future<List<DataSegment>>> segments = new HashMap<>();

    for (final HadoopIngestionSpec specIn : specs) {
      Future<List<DataSegment>> future = executor.submit(
          new Callable<List<DataSegment>>()
          {
            public List<DataSegment> call() throws Exception
            {
              HadoopIngestionSpec spec = specIn;
              boolean determineIntervals = !spec.getDataSchema().getGranularitySpec().bucketIntervals().isPresent();

              spec = HadoopIngestionSpec.updateSegmentListIfDatasourcePathSpecIsUsed(
                  spec,
                  jsonMapper,
                  new OverlordActionBasedUsedSegmentLister(toolbox)
              );

              final String config = invokeForeignLoader(
                  "io.druid.hadoopMultiIndexTaskExt.HadoopMultiIndexTask$HadoopDetermineConfigInnerProcessing",
                  new String[]{
                      toolbox.getObjectMapper().writeValueAsString(spec),
                      toolbox.getConfig().getHadoopWorkingPath(),
                      toolbox.getSegmentPusher().getPathForHadoop()
                  },
                  loader
              );

              final HadoopIngestionSpec indexerSchema = toolbox
                  .getObjectMapper()
                  .readValue(config, HadoopIngestionSpec.class);


              // We should have a lock from before we started running only if interval was specified
              String version = null;
              if (determineIntervals) {
                Interval interval = JodaUtils.umbrellaInterval(
                    JodaUtils.condenseIntervals(
                        indexerSchema.getDataSchema().getGranularitySpec().bucketIntervals().get()
                    )
                );
                TaskLock lock = toolbox.getTaskActionClient().submit(
                    new DataSourcesLockAcquireAction(
                        ImmutableList.of(
                            new DataSourceAndInterval(
                                spec.getDataSchema().getDataSource(),
                                interval
                            )
                        )
                    )
                ).get(0);
                version = lock.getVersion();
              } else {
                for (TaskLock tl : getTaskLocks(toolbox)) {
                  if (tl.getDataSource().equals(spec.getDataSchema().getDataSource())) {
                    version = tl.getVersion();
                    break;
                  }
                }
              }

              if (version == null) {
                throw new ISE("Failed to get lock version for [%s]", spec.getDataSchema().getDataSource());
              }

              log.info("Setting version to: %s", version);

              final String segments = invokeForeignLoader(
                  "io.druid.hadoopMultiIndexTaskExt.HadoopMultiIndexTask$HadoopIndexGeneratorInnerProcessing",
                  new String[]{
                      toolbox.getObjectMapper().writeValueAsString(indexerSchema),
                      version
                  },
                  loader
              );

              if (segments != null) {
                log.info(
                    "For dataSource [%s], segment to publish are [%s].",
                    indexerSchema.getDataSchema().getDataSource(),
                    segments
                );

                return toolbox.getObjectMapper().readValue(
                    segments,
                    new TypeReference<List<DataSegment>>()
                    {
                    }
                );
              } else {
                return null;
              }
            }
          }
      );

      segments.put(specIn.getDataSchema().getDataSource(), future);
    }

    boolean success = true;
    Map<String, List<DataSegment>> segmentsToPublish = new HashMap<>(segments.size());
    for (Map.Entry<String, Future<List<DataSegment>>> e : segments.entrySet()) {
      List<DataSegment> segs = e.getValue().get();
      if (segs != null && segs.size() > 0) {
        segmentsToPublish.put(e.getKey(), segs);
      } else {
        success = false;
        log.error("WTF! No segments created for dataSource [%s].", e.getKey());
      }
    }

    if (success) {
      toolbox.publishSegments(segmentsToPublish);
      log.info("All segments were published successfull.");

      return TaskStatus.success(getId());
    } else {
      log.error("No segments are created for one or more daaSources. Failing the Task.");
      return TaskStatus.failure(getId());
    }
  }

  public static class HadoopIndexGeneratorInnerProcessing
  {
    public static String runTask(String[] args) throws Exception
    {
      final String schema = args[0];
      String version = args[1];

      final HadoopIngestionSpec theSchema = HadoopDruidIndexerConfig.JSON_MAPPER
          .readValue(
              schema,
              HadoopIngestionSpec.class
          );
      final HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromSpec(
          theSchema
              .withTuningConfig(theSchema.getTuningConfig().withVersion(version))
      );

      // MetadataStorageUpdaterJobHandler is only needed when running standalone without indexing service
      // In that case the whatever runs the Hadoop Index Task must ensure MetadataStorageUpdaterJobHandler
      // can be injected based on the configuration given in config.getSchema().getIOConfig().getMetadataUpdateSpec()
      final MetadataStorageUpdaterJobHandler maybeHandler;
      if (config.isUpdaterJobSpecSet()) {
        maybeHandler = injector.getInstance(MetadataStorageUpdaterJobHandler.class);
      } else {
        maybeHandler = null;
      }
      HadoopDruidIndexerJob job = new HadoopDruidIndexerJob(config, maybeHandler);

      log.info("Starting a hadoop index generator job...");
      if (job.run()) {
        return HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(job.getPublishedSegments());
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

      final HadoopIngestionSpec theSchema = HadoopDruidIndexerConfig.JSON_MAPPER
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
        return HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(config.getSchema());
      }

      return null;
    }
  }
}

