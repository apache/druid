/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

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
import io.druid.indexing.common.actions.LockAcquireAction;
import io.druid.indexing.common.actions.LockTryAcquireAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.hadoop.OverlordActionBasedUsedSegmentLister;
import io.druid.java.util.common.logger.Logger;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;

public class HadoopIndexTask extends HadoopTask
{
  private static final Logger log = new Logger(HadoopIndexTask.class);

  private static String getTheDataSource(HadoopIngestionSpec spec)
  {
    return spec.getDataSchema().getDataSource();
  }

  @JsonIgnore
  private HadoopIngestionSpec spec;

  @JsonIgnore
  private final String classpathPrefix;

  @JsonIgnore
  private final ObjectMapper jsonMapper;

  /**
   * @param spec is used by the HadoopDruidIndexerJob to set up the appropriate parameters
   *             for creating Druid index segments. It may be modified.
   *             <p/>
   *             Here, we will ensure that the DbConnectorConfig field of the spec is set to null, such that the
   *             job does not push a list of published segments the database. Instead, we will use the method
   *             IndexGeneratorJob.getPublishedSegments() to simply return a list of the published
   *             segments, and let the indexing service report these segments to the database.
   */

  @JsonCreator
  public HadoopIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("spec") HadoopIngestionSpec spec,
      @JsonProperty("hadoopCoordinates") String hadoopCoordinates,
      @JsonProperty("hadoopDependencyCoordinates") List<String> hadoopDependencyCoordinates,
      @JsonProperty("classpathPrefix") String classpathPrefix,
      @JacksonInject ObjectMapper jsonMapper,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        id != null ? id : String.format("index_hadoop_%s_%s", getTheDataSource(spec), new DateTime()),
        getTheDataSource(spec),
        hadoopDependencyCoordinates == null
        ? (hadoopCoordinates == null ? null : ImmutableList.of(hadoopCoordinates))
        : hadoopDependencyCoordinates,
        context
    );


    this.spec = spec;

    // Some HadoopIngestionSpec stuff doesn't make sense in the context of the indexing service
    Preconditions.checkArgument(
        this.spec.getIOConfig().getSegmentOutputPath() == null,
        "segmentOutputPath must be absent"
    );
    Preconditions.checkArgument(this.spec.getTuningConfig().getWorkingPath() == null, "workingPath must be absent");
    Preconditions.checkArgument(
        this.spec.getIOConfig().getMetadataUpdateSpec() == null,
        "metadataUpdateSpec must be absent"
    );

    this.classpathPrefix = classpathPrefix;
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "null ObjectMappper");
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
      return taskActionClient.submit(new LockTryAcquireAction(interval)) != null;
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
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    final ClassLoader loader = buildClassLoader(toolbox);
    boolean determineIntervals = !spec.getDataSchema().getGranularitySpec().bucketIntervals().isPresent();

    spec = HadoopIngestionSpec.updateSegmentListIfDatasourcePathSpecIsUsed(
        spec,
        jsonMapper,
        new OverlordActionBasedUsedSegmentLister(toolbox)
    );

    final String config = invokeForeignLoader(
        "io.druid.indexing.common.task.HadoopIndexTask$HadoopDetermineConfigInnerProcessing",
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
    String version;
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

    final String specVersion = indexerSchema.getTuningConfig().getVersion();
    if (indexerSchema.getTuningConfig().isUseExplicitVersion()) {
      if (specVersion.compareTo(version) < 0) {
        version = specVersion;
      } else {
        log.error(
            "Spec version can not be greater than or equal to the lock version, Spec version: [%s] Lock version: [%s].",
            specVersion,
            version
        );
        return TaskStatus.failure(getId());
      }
    }

    log.info("Setting version to: %s", version);

    final String segments = invokeForeignLoader(
        "io.druid.indexing.common.task.HadoopIndexTask$HadoopIndexGeneratorInnerProcessing",
        new String[]{
            toolbox.getObjectMapper().writeValueAsString(indexerSchema),
            version
        },
        loader
    );

    if (segments != null) {
      List<DataSegment> publishedSegments = toolbox.getObjectMapper().readValue(
          segments,
          new TypeReference<List<DataSegment>>()
          {
          }
      );

      toolbox.publishSegments(publishedSegments);
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
