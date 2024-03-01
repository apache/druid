/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.commons.lang.BooleanUtils;
import org.apache.druid.indexer.DataSegmentAndIndexZipFilePath;
import org.apache.druid.indexer.HadoopDruidDetermineConfigurationJob;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.indexer.HadoopDruidIndexerJob;
import org.apache.druid.indexer.HadoopIngestionSpec;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexer.JobHelper;
import org.apache.druid.indexer.MetadataStorageUpdaterJobHandler;
import org.apache.druid.indexer.TaskMetricsGetter;
import org.apache.druid.indexer.TaskMetricsUtils;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockAcquireAction;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.hadoop.OverlordActionBasedUsedSegmentsRetriever;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.timeline.DataSegment;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class HadoopIndexTask extends HadoopTask implements ChatHandler
{
  public static final String INPUT_SOURCE_TYPE = "hadoop";
  private static final Logger log = new Logger(HadoopIndexTask.class);
  private static final String HADOOP_JOB_ID_FILENAME = "mapReduceJobId.json";
  private static final String TYPE = "index_hadoop";
  private TaskConfig taskConfig = null;

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

  @JsonIgnore
  private final AuthorizerMapper authorizerMapper;

  @JsonIgnore
  private final Optional<ChatHandlerProvider> chatHandlerProvider;

  @JsonIgnore
  private InnerProcessingStatsGetter determinePartitionsStatsGetter;

  @JsonIgnore
  private InnerProcessingStatsGetter buildSegmentsStatsGetter;

  @JsonIgnore
  private IngestionState ingestionState;

  @JsonIgnore
  private HadoopDetermineConfigInnerProcessingStatus determineConfigStatus = null;

  @JsonIgnore
  private HadoopIndexGeneratorInnerProcessingStatus buildSegmentsStatus = null;

  @JsonIgnore
  private String errorMsg;

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
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject ChatHandlerProvider chatHandlerProvider
  )
  {
    super(
        getOrMakeId(id, TYPE, getTheDataSource(spec)),
        getTheDataSource(spec),
        hadoopDependencyCoordinates == null
        ? (hadoopCoordinates == null ? null : ImmutableList.of(hadoopCoordinates))
        : hadoopDependencyCoordinates,
        context
    );
    this.authorizerMapper = authorizerMapper;
    this.chatHandlerProvider = Optional.fromNullable(chatHandlerProvider);
    this.spec = context == null ? spec : spec.withContext(context);

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
    this.ingestionState = IngestionState.NOT_STARTED;
  }

  @Override
  public String getType()
  {
    return "index_hadoop";
  }

  @Nonnull
  @JsonIgnore
  @Override
  public Set<ResourceAction> getInputSourceResources()
  {
    return Collections.singleton(new ResourceAction(new Resource(INPUT_SOURCE_TYPE, ResourceType.EXTERNAL), Action.READ));
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    Iterable<Interval> intervals = spec.getDataSchema().getGranularitySpec().sortedBucketIntervals();
    if (intervals.iterator().hasNext()) {
      Interval interval = JodaUtils.umbrellaInterval(
          JodaUtils.condenseIntervals(intervals)
      );
      final TaskLock lock = taskActionClient.submit(
          new TimeChunkLockTryAcquireAction(
              TaskLockType.EXCLUSIVE,
              interval
          )
      );
      if (lock == null) {
        return false;
      }
      if (lock.isRevoked()) {
        throw new ISE(StringUtils.format("Lock for interval [%s] was revoked.", interval));
      }
      return true;
    } else {
      return true;
    }
  }

  @Override
  public boolean requireLockExistingSegments()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<DataSegment> findSegmentsToLock(TaskActionClient taskActionClient, List<Interval> intervals)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isPerfectRollup()
  {
    return true;
  }

  @Nullable
  @Override
  public Granularity getSegmentGranularity()
  {
    final GranularitySpec granularitySpec = spec.getDataSchema().getGranularitySpec();
    if (granularitySpec instanceof ArbitraryGranularitySpec) {
      return null;
    } else {
      return granularitySpec.getSegmentGranularity();
    }
  }

  @JsonProperty("spec")
  public HadoopIngestionSpec getSpec()
  {
    return spec;
  }

  @Override
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

  private String getHadoopJobIdFileName()
  {
    return getHadoopJobIdFile().getAbsolutePath();
  }

  private boolean hadoopJobIdFileExists()
  {
    return getHadoopJobIdFile().exists();
  }

  private File getHadoopJobIdFile()
  {
    return new File(taskConfig.getTaskDir(getId()), HADOOP_JOB_ID_FILENAME);
  }

  @Override
  public TaskStatus runTask(TaskToolbox toolbox)
  {
    try {
      taskConfig = toolbox.getConfig();
      if (chatHandlerProvider.isPresent()) {
        log.info("Found chat handler of class[%s]", chatHandlerProvider.get().getClass().getName());
        chatHandlerProvider.get().register(getId(), this, false);
      } else {
        log.warn("No chat handler detected");
      }

      return runInternal(toolbox);
    }
    catch (Exception e) {
      Throwable effectiveException;
      if (e instanceof RuntimeException && e.getCause() instanceof InvocationTargetException) {
        InvocationTargetException ite = (InvocationTargetException) e.getCause();
        effectiveException = ite.getCause();
        log.error(effectiveException, "Got invocation target exception in run()");
      } else {
        effectiveException = e;
        log.error(e, "Encountered exception in run()");
      }

      errorMsg = Throwables.getStackTraceAsString(effectiveException);
      toolbox.getTaskReportFileWriter().write(getId(), getTaskCompletionReports());
      return TaskStatus.failure(
          getId(),
          errorMsg
      );
    }
    finally {
      if (chatHandlerProvider.isPresent()) {
        chatHandlerProvider.get().unregister(getId());
      }
    }
  }

  @SuppressWarnings("unchecked")
  private TaskStatus runInternal(TaskToolbox toolbox) throws Exception
  {
    boolean indexGeneratorJobAttempted = false;
    boolean indexGeneratorJobSuccess = false;
    HadoopIngestionSpec indexerSchema = null;
    try {
      registerResourceCloserOnAbnormalExit(config -> killHadoopJob());
      String hadoopJobIdFile = getHadoopJobIdFileName();
      logExtensionsConfig();
      final ClassLoader loader = buildClassLoader(toolbox);
      boolean determineIntervals = spec.getDataSchema().getGranularitySpec().inputIntervals().isEmpty();

      HadoopIngestionSpec.updateSegmentListIfDatasourcePathSpecIsUsed(
          spec,
          jsonMapper,
          new OverlordActionBasedUsedSegmentsRetriever(toolbox)
      );

      Object determinePartitionsInnerProcessingRunner = getForeignClassloaderObject(
          "org.apache.druid.indexing.common.task.HadoopIndexTask$HadoopDetermineConfigInnerProcessingRunner",
          loader
      );
      determinePartitionsStatsGetter = new InnerProcessingStatsGetter(determinePartitionsInnerProcessingRunner);

      String[] determinePartitionsInput = new String[]{
          toolbox.getJsonMapper().writeValueAsString(spec),
          toolbox.getConfig().getHadoopWorkingPath(),
          toolbox.getSegmentPusher().getPathForHadoop(),
          hadoopJobIdFile
      };

      final ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
      Class<?> determinePartitionsRunnerClass = determinePartitionsInnerProcessingRunner.getClass();
      Method determinePartitionsInnerProcessingRunTask = determinePartitionsRunnerClass.getMethod(
          "runTask",
          determinePartitionsInput.getClass()
      );
      try {
        Thread.currentThread().setContextClassLoader(loader);

        ingestionState = IngestionState.DETERMINE_PARTITIONS;

        final String determineConfigStatusString = (String) determinePartitionsInnerProcessingRunTask.invoke(
            determinePartitionsInnerProcessingRunner,
            new Object[]{determinePartitionsInput}
        );


        determineConfigStatus = toolbox
            .getJsonMapper()
            .readValue(determineConfigStatusString, HadoopDetermineConfigInnerProcessingStatus.class);

        indexerSchema = determineConfigStatus.getSchema();
        if (indexerSchema == null) {
          errorMsg = determineConfigStatus.getErrorMsg();
          toolbox.getTaskReportFileWriter().write(getId(), getTaskCompletionReports());
          return TaskStatus.failure(
              getId(),
              errorMsg
          );
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
      finally {
        Thread.currentThread().setContextClassLoader(oldLoader);
      }

      // We should have a lock from before we started running only if interval was specified
      String version;
      if (determineIntervals) {
        Interval interval = JodaUtils.umbrellaInterval(
            JodaUtils.condenseIntervals(
                indexerSchema.getDataSchema().getGranularitySpec().sortedBucketIntervals()
            )
        );
        final long lockTimeoutMs = getContextValue(Tasks.LOCK_TIMEOUT_KEY, Tasks.DEFAULT_LOCK_TIMEOUT_MILLIS);
        // Note: if lockTimeoutMs is larger than ServerConfig.maxIdleTime, the below line can incur http timeout error.
        final TaskLock lock = Preconditions.checkNotNull(
            toolbox.getTaskActionClient().submit(
                new TimeChunkLockAcquireAction(TaskLockType.EXCLUSIVE, interval, lockTimeoutMs)
            ),
            "Cannot acquire a lock for interval[%s]", interval
        );
        if (lock.isRevoked()) {
          throw new ISE(StringUtils.format("Lock for interval [%s] was revoked.", interval));
        }
        version = lock.getVersion();
      } else {
        Iterable<TaskLock> locks = getTaskLocks(toolbox.getTaskActionClient());
        final TaskLock myLock = Iterables.getOnlyElement(locks);
        version = myLock.getVersion();
      }

      final String specVersion = indexerSchema.getTuningConfig().getVersion();
      if (indexerSchema.getTuningConfig().isUseExplicitVersion()) {
        if (specVersion.compareTo(version) < 0) {
          version = specVersion;
        } else {
          String errMsg =
              StringUtils.format(
                  "Spec version can not be greater than or equal to the lock version, Spec version: [%s] Lock version: [%s].",
                  specVersion,
                  version
              );
          log.error(errMsg);
          toolbox.getTaskReportFileWriter().write(getId(), null);
          return TaskStatus.failure(getId(), errMsg);
        }
      }

      log.info("Setting version to: %s", version);

      Object innerProcessingRunner = getForeignClassloaderObject(
          "org.apache.druid.indexing.common.task.HadoopIndexTask$HadoopIndexGeneratorInnerProcessingRunner",
          loader
      );
      buildSegmentsStatsGetter = new InnerProcessingStatsGetter(innerProcessingRunner);

      String[] buildSegmentsInput = new String[]{
          toolbox.getJsonMapper().writeValueAsString(indexerSchema),
          version,
          hadoopJobIdFile
      };

      Class<?> buildSegmentsRunnerClass = innerProcessingRunner.getClass();
      Method innerProcessingRunTask = buildSegmentsRunnerClass.getMethod("runTask", buildSegmentsInput.getClass());

      try {
        Thread.currentThread().setContextClassLoader(loader);

        ingestionState = IngestionState.BUILD_SEGMENTS;
        indexGeneratorJobAttempted = true;
        final String jobStatusString = (String) innerProcessingRunTask.invoke(
            innerProcessingRunner,
            new Object[]{buildSegmentsInput}
        );

        buildSegmentsStatus = toolbox.getJsonMapper().readValue(
            jobStatusString,
            HadoopIndexGeneratorInnerProcessingStatus.class
        );

        List<DataSegmentAndIndexZipFilePath> dataSegmentAndIndexZipFilePaths = buildSegmentsStatus.getDataSegmentAndIndexZipFilePaths();
        if (dataSegmentAndIndexZipFilePaths != null) {
          indexGeneratorJobSuccess = true;
          renameSegmentIndexFilesJob(
              toolbox.getJsonMapper().writeValueAsString(indexerSchema),
              toolbox.getJsonMapper().writeValueAsString(dataSegmentAndIndexZipFilePaths)
          );

          ArrayList<DataSegment> segments = new ArrayList<>(dataSegmentAndIndexZipFilePaths.stream()
                                                                                           .map(
                                                                                               DataSegmentAndIndexZipFilePath::getSegment)
                                                                                           .collect(Collectors.toList()));
          toolbox.publishSegments(segments);

          // Try to wait for segments to be loaded by the cluster if the tuning config specifies a non-zero value
          // for awaitSegmentAvailabilityTimeoutMillis
          if (spec.getTuningConfig().getAwaitSegmentAvailabilityTimeoutMillis() > 0) {
            ingestionState = IngestionState.SEGMENT_AVAILABILITY_WAIT;
            waitForSegmentAvailability(
                toolbox,
                segments,
                spec.getTuningConfig().getAwaitSegmentAvailabilityTimeoutMillis()
            );
          }

          ingestionState = IngestionState.COMPLETED;
          toolbox.getTaskReportFileWriter().write(getId(), getTaskCompletionReports());
          return TaskStatus.success(getId());
        } else {
          errorMsg = buildSegmentsStatus.getErrorMsg();
          toolbox.getTaskReportFileWriter().write(getId(), getTaskCompletionReports());
          return TaskStatus.failure(
              getId(),
              errorMsg
          );
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
      finally {
        Thread.currentThread().setContextClassLoader(oldLoader);
      }
    }
    finally {
      indexerGeneratorCleanupJob(
          indexGeneratorJobAttempted,
          indexGeneratorJobSuccess,
          indexerSchema == null ? null : toolbox.getJsonMapper().writeValueAsString(indexerSchema)
      );
    }
  }

  private void killHadoopJob()
  {
    // To avoid issue of kill command once the ingestion task is actually completed
    if (hadoopJobIdFileExists() && !ingestionState.equals(IngestionState.COMPLETED)) {
      final ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
      String hadoopJobIdFile = getHadoopJobIdFileName();

      try {
        ClassLoader loader = HadoopTask.buildClassLoader(
            getHadoopDependencyCoordinates(),
            taskConfig.getDefaultHadoopCoordinates()
        );

        Object killMRJobInnerProcessingRunner = getForeignClassloaderObject(
            "org.apache.druid.indexing.common.task.HadoopIndexTask$HadoopKillMRJobIdProcessingRunner",
            loader
        );

        String[] buildKillJobInput = new String[]{hadoopJobIdFile};

        Class<?> buildKillJobRunnerClass = killMRJobInnerProcessingRunner.getClass();
        Method innerProcessingRunTask = buildKillJobRunnerClass.getMethod("runTask", buildKillJobInput.getClass());

        Thread.currentThread().setContextClassLoader(loader);
        final String[] killStatusString = (String[]) innerProcessingRunTask.invoke(
            killMRJobInnerProcessingRunner,
            new Object[]{buildKillJobInput}
        );

        log.info(StringUtils.format("Tried killing job: [%s], status: [%s]", killStatusString[0], killStatusString[1]));
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
      finally {
        Thread.currentThread().setContextClassLoader(oldLoader);
      }
    }
  }

  /**
   * Must be called only when the hadoopy classloader is the current classloader
   */
  private void renameSegmentIndexFilesJob(
      String hadoopIngestionSpecStr,
      String dataSegmentAndIndexZipFilePathListStr
  )
  {
    final ClassLoader loader = Thread.currentThread().getContextClassLoader();
    try {
      final Class<?> clazz = loader.loadClass(
          "org.apache.druid.indexing.common.task.HadoopIndexTask$HadoopRenameSegmentIndexFilesRunner"
      );
      Object renameSegmentIndexFilesRunner = clazz.newInstance();

      String[] renameSegmentIndexFilesJobInput = new String[]{
          hadoopIngestionSpecStr,
          dataSegmentAndIndexZipFilePathListStr
      };

      Class<?> buildRenameSegmentIndexFilesJobRunnerClass = renameSegmentIndexFilesRunner.getClass();
      Method renameSegmentIndexFiles = buildRenameSegmentIndexFilesJobRunnerClass.getMethod(
          "runTask",
          renameSegmentIndexFilesJobInput.getClass()
      );

      renameSegmentIndexFiles.invoke(
          renameSegmentIndexFilesRunner,
          new Object[]{renameSegmentIndexFilesJobInput}
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void indexerGeneratorCleanupJob(
      boolean indexGeneratorJobAttempted,
      boolean indexGeneratorJobSuccess,
      String hadoopIngestionSpecStr
  )
  {
    if (!indexGeneratorJobAttempted) {
      log.info("No need for cleanup as index generator job did not even run");
      return;
    }

    final ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
    try {
      ClassLoader loader = HadoopTask.buildClassLoader(
          getHadoopDependencyCoordinates(),
          taskConfig.getDefaultHadoopCoordinates()
      );

      Object indexerGeneratorCleanupRunner = getForeignClassloaderObject(
          "org.apache.druid.indexing.common.task.HadoopIndexTask$HadoopIndexerGeneratorCleanupRunner",
          loader
      );

      String[] indexerGeneratorCleanupJobInput = new String[]{
          indexGeneratorJobSuccess ? "true" : "false",
          hadoopIngestionSpecStr,
      };

      Class<?> buildIndexerGeneratorCleanupRunnerClass = indexerGeneratorCleanupRunner.getClass();
      Method indexerGeneratorCleanup = buildIndexerGeneratorCleanupRunnerClass.getMethod(
          "runTask",
          indexerGeneratorCleanupJobInput.getClass()
      );

      Thread.currentThread().setContextClassLoader(loader);
      indexerGeneratorCleanup.invoke(
          indexerGeneratorCleanupRunner,
          new Object[]{indexerGeneratorCleanupJobInput}
      );
    }
    catch (Exception e) {
      log.warn(e, "Failed to cleanup after index generator job");
    }
    finally {
      Thread.currentThread().setContextClassLoader(oldLoader);
    }
  }

  @GET
  @Path("/rowStats")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRowStats(
      @Context final HttpServletRequest req,
      @QueryParam("windows") List<Integer> windows
  )
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    Map<String, Object> returnMap = new HashMap<>();
    Map<String, Object> totalsMap = new HashMap<>();

    if (determinePartitionsStatsGetter != null) {
      totalsMap.put(RowIngestionMeters.DETERMINE_PARTITIONS, determinePartitionsStatsGetter.getTotalMetrics());
    }

    if (buildSegmentsStatsGetter != null) {
      totalsMap.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegmentsStatsGetter.getTotalMetrics());
    }

    returnMap.put("totals", totalsMap);
    return Response.ok(returnMap).build();
  }

  private Map<String, TaskReport> getTaskCompletionReports()
  {
    return TaskReport.buildTaskReports(
        new IngestionStatsAndErrorsTaskReport(
            getId(),
            new IngestionStatsAndErrorsTaskReportData(
                ingestionState,
                null,
                getTaskCompletionRowStats(),
                errorMsg,
                segmentAvailabilityConfirmationCompleted,
                segmentAvailabilityWaitTimeMs,
                Collections.emptyMap()
            )
        )
    );
  }

  private Map<String, Object> getTaskCompletionRowStats()
  {
    Map<String, Object> metrics = new HashMap<>();
    if (determineConfigStatus != null) {
      metrics.put(
          RowIngestionMeters.DETERMINE_PARTITIONS,
          determineConfigStatus.getMetrics()
      );
    }
    if (buildSegmentsStatus != null) {
      metrics.put(
          RowIngestionMeters.BUILD_SEGMENTS,
          buildSegmentsStatus.getMetrics()
      );
    }
    return metrics;
  }

  public static class InnerProcessingStatsGetter implements TaskMetricsGetter
  {
    static final List<String> KEYS = ImmutableList.of(
        TaskMetricsUtils.ROWS_PROCESSED,
        TaskMetricsUtils.ROWS_PROCESSED_WITH_ERRORS,
        TaskMetricsUtils.ROWS_THROWN_AWAY,
        TaskMetricsUtils.ROWS_UNPARSEABLE
    );

    private final Method getStatsMethod;
    private final Object innerProcessingRunner;

    public InnerProcessingStatsGetter(
        Object innerProcessingRunner
    )
    {
      try {
        Class<?> aClazz = innerProcessingRunner.getClass();
        this.getStatsMethod = aClazz.getMethod("getStats");
        this.innerProcessingRunner = innerProcessingRunner;
      }
      catch (NoSuchMethodException nsme) {
        throw new RuntimeException(nsme);
      }
    }

    @Override
    public List<String> getKeys()
    {
      return KEYS;
    }

    @Nullable
    @Override
    public Map<String, Number> getTotalMetrics()
    {
      try {
        Map<String, Object> statsMap = (Map<String, Object>) getStatsMethod.invoke(innerProcessingRunner);
        if (statsMap == null) {
          return null;
        }
        long curProcessed = (Long) statsMap.get(TaskMetricsUtils.ROWS_PROCESSED);
        long curProcessedWithErrors = (Long) statsMap.get(TaskMetricsUtils.ROWS_PROCESSED_WITH_ERRORS);
        long curThrownAway = (Long) statsMap.get(TaskMetricsUtils.ROWS_THROWN_AWAY);
        long curUnparseable = (Long) statsMap.get(TaskMetricsUtils.ROWS_UNPARSEABLE);

        return ImmutableMap.of(
            TaskMetricsUtils.ROWS_PROCESSED, curProcessed,
            TaskMetricsUtils.ROWS_PROCESSED_WITH_ERRORS, curProcessedWithErrors,
            TaskMetricsUtils.ROWS_THROWN_AWAY, curThrownAway,
            TaskMetricsUtils.ROWS_UNPARSEABLE, curUnparseable
        );
      }
      catch (Exception e) {
        log.error(e, "Got exception from getTotalMetrics()");
        return null;
      }
    }
  }


  /**
   * Called indirectly in {@link HadoopIndexTask#run(TaskToolbox)}.
   */
  @SuppressWarnings("unused")
  public static class HadoopDetermineConfigInnerProcessingRunner
  {
    private HadoopDruidDetermineConfigurationJob job;

    public String runTask(String[] args) throws Exception
    {
      final String schema = args[0];
      final String workingPath = args[1];
      final String segmentOutputPath = args[2];
      final String hadoopJobIdFile = args[3];

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

      job = new HadoopDruidDetermineConfigurationJob(config);
      job.setHadoopJobIdFile(hadoopJobIdFile);

      log.info("Starting a hadoop determine configuration job...");
      if (job.run()) {
        return HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(
            new HadoopDetermineConfigInnerProcessingStatus(config.getSchema(), job.getStats(), null)
        );
      } else {
        return HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(
            new HadoopDetermineConfigInnerProcessingStatus(null, job.getStats(), job.getErrorMessage())
        );
      }
    }

    public Map<String, Object> getStats()
    {
      if (job == null) {
        return null;
      }

      return job.getStats();
    }
  }

  @SuppressWarnings("unused")
  public static class HadoopIndexGeneratorInnerProcessingRunner
  {
    private HadoopDruidIndexerJob job;

    public String runTask(String[] args) throws Exception
    {
      final String schema = args[0];
      String version = args[1];
      final String hadoopJobIdFile = args[2];

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
        maybeHandler = INJECTOR.getInstance(MetadataStorageUpdaterJobHandler.class);
      } else {
        maybeHandler = null;
      }
      job = new HadoopDruidIndexerJob(config, maybeHandler);
      job.setHadoopJobIdFile(hadoopJobIdFile);

      log.info("Starting a hadoop index generator job...");
      try {
        if (job.run()) {
          return HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(
              new HadoopIndexGeneratorInnerProcessingStatus(
                  job.getPublishedSegmentAndIndexZipFilePaths(),
                  job.getStats(),
                  null
              )
          );
        } else {
          return HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(
              new HadoopIndexGeneratorInnerProcessingStatus(
                  null,
                  job.getStats(),
                  job.getErrorMessage()
              )
          );
        }
      }
      catch (Exception e) {
        log.error(e, "Encountered exception in HadoopIndexGeneratorInnerProcessing.");
        return HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(
            new HadoopIndexGeneratorInnerProcessingStatus(
                null,
                job.getStats(),
                e.getMessage()
            )
        );
      }
    }

    public Map<String, Object> getStats()
    {
      if (job == null) {
        return null;
      }

      return job.getStats();
    }
  }

  @SuppressWarnings("unused")
  public static class HadoopKillMRJobIdProcessingRunner
  {
    public String[] runTask(String[] args) throws Exception
    {
      File hadoopJobIdFile = new File(args[0]);
      String jobId = null;

      try {
        if (hadoopJobIdFile.exists()) {
          jobId = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(hadoopJobIdFile, String.class);
        }
      }
      catch (Exception e) {
        log.warn(e, "exeption while reading hadoop job id from: [%s]", hadoopJobIdFile);
      }

      if (jobId != null) {
        // This call to JobHelper#authenticate will be transparent if already authenticated or using inseucre Hadoop.
        JobHelper.authenticate();
        int res = ToolRunner.run(new JobClient(), new String[]{
            "-kill",
            jobId
        });

        return new String[]{jobId, (res == 0 ? "Success" : "Fail")};
      }
      return new String[]{jobId, "Fail"};
    }
  }

  @SuppressWarnings("unused")
  public static class HadoopRenameSegmentIndexFilesRunner
  {
    TypeReference<List<DataSegmentAndIndexZipFilePath>> LIST_DATA_SEGMENT_AND_INDEX_ZIP_FILE_PATH =
        new TypeReference<List<DataSegmentAndIndexZipFilePath>>()
        {
        };

    public void runTask(String[] args) throws Exception
    {
      if (args.length != 2) {
        log.warn("HadoopRenameSegmentIndexFilesRunner called with improper number of arguments");
      }
      String hadoopIngestionSpecStr = args[0];
      String dataSegmentAndIndexZipFilePathListStr = args[1];

      HadoopIngestionSpec indexerSchema;
      List<DataSegmentAndIndexZipFilePath> dataSegmentAndIndexZipFilePaths;
      try {
        indexerSchema = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(
            hadoopIngestionSpecStr,
            HadoopIngestionSpec.class
        );
        dataSegmentAndIndexZipFilePaths = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(
            dataSegmentAndIndexZipFilePathListStr,
            LIST_DATA_SEGMENT_AND_INDEX_ZIP_FILE_PATH
        );
      }
      catch (Exception e) {
        log.warn(
            e,
            "HadoopRenameSegmentIndexFilesRunner: Error occurred while trying to read input parameters into data objects"
        );
        throw e;
      }
      JobHelper.renameIndexFilesForSegments(
          indexerSchema,
          dataSegmentAndIndexZipFilePaths
      );
    }
  }

  @SuppressWarnings("unused")
  public static class HadoopIndexerGeneratorCleanupRunner
  {
    TypeReference<List<DataSegmentAndIndexZipFilePath>> LIST_DATA_SEGMENT_AND_INDEX_ZIP_FILE_PATH =
        new TypeReference<List<DataSegmentAndIndexZipFilePath>>()
        {
        };

    public void runTask(String[] args) throws Exception
    {
      if (args.length != 2) {
        log.warn("HadoopIndexerGeneratorCleanupRunner called with improper number of arguments");
      }

      String indexGeneratorJobSucceededStr = args[0];
      String hadoopIngestionSpecStr = args[1];

      HadoopIngestionSpec indexerSchema;
      boolean indexGeneratorJobSucceeded;
      List<DataSegmentAndIndexZipFilePath> dataSegmentAndIndexZipFilePaths;
      try {
        indexerSchema = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(
            hadoopIngestionSpecStr,
            HadoopIngestionSpec.class
        );
        indexGeneratorJobSucceeded = BooleanUtils.toBoolean(indexGeneratorJobSucceededStr);
      }
      catch (Exception e) {
        log.warn(
            e,
            "HadoopIndexerGeneratorCleanupRunner: Error occurred while trying to read input parameters into data objects"
        );
        throw e;
      }
      JobHelper.maybeDeleteIntermediatePath(
          indexGeneratorJobSucceeded,
          indexerSchema
      );
    }
  }

  public static class HadoopIndexGeneratorInnerProcessingStatus
  {
    private final List<DataSegmentAndIndexZipFilePath> dataSegmentAndIndexZipFilePaths;
    private final Map<String, Object> metrics;
    private final String errorMsg;

    @JsonCreator
    public HadoopIndexGeneratorInnerProcessingStatus(
        @JsonProperty("dataSegmentAndIndexZipFilePaths") List<DataSegmentAndIndexZipFilePath> dataSegmentAndIndexZipFilePaths,
        @JsonProperty("metrics") Map<String, Object> metrics,
        @JsonProperty("errorMsg") String errorMsg
    )
    {
      this.dataSegmentAndIndexZipFilePaths = dataSegmentAndIndexZipFilePaths;
      this.metrics = metrics;
      this.errorMsg = errorMsg;
    }

    @JsonProperty
    public List<DataSegmentAndIndexZipFilePath> getDataSegmentAndIndexZipFilePaths()
    {
      return dataSegmentAndIndexZipFilePaths;
    }

    @JsonProperty
    public Map<String, Object> getMetrics()
    {
      return metrics;
    }

    @JsonProperty
    public String getErrorMsg()
    {
      return errorMsg;
    }
  }

  public static class HadoopDetermineConfigInnerProcessingStatus
  {
    private final HadoopIngestionSpec schema;
    private final Map<String, Object> metrics;
    private final String errorMsg;

    @JsonCreator
    public HadoopDetermineConfigInnerProcessingStatus(
        @JsonProperty("schema") HadoopIngestionSpec schema,
        @JsonProperty("metrics") Map<String, Object> metrics,
        @JsonProperty("errorMsg") String errorMsg
    )
    {
      this.schema = schema;
      this.metrics = metrics;
      this.errorMsg = errorMsg;
    }

    @JsonProperty
    public HadoopIngestionSpec getSchema()
    {
      return schema;
    }

    @JsonProperty
    public Map<String, Object> getMetrics()
    {
      return metrics;
    }

    @JsonProperty
    public String getErrorMsg()
    {
      return errorMsg;
    }
  }
}
