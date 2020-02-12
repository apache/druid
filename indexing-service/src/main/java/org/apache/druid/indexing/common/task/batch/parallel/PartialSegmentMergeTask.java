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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.SurrogateAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.ClientBasedTaskInfoProvider;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamResponseHandler;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.utils.CompressionUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Base class for creating task that merges partial segments created by {@link PartialSegmentGenerateTask}.
 */
abstract class PartialSegmentMergeTask<S extends ShardSpec, P extends PartitionLocation> extends PerfectRollupWorkerTask
{
  private static final Logger LOG = new Logger(PartialSegmentMergeTask.class);
  private static final int BUFFER_SIZE = 1024 * 4;
  private static final int NUM_FETCH_RETRIES = 3;

  private final PartialSegmentMergeIOConfig<P> ioConfig;
  private final int numAttempts;
  private final String supervisorTaskId;
  private final IndexingServiceClient indexingServiceClient;
  private final IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> taskClientFactory;
  private final HttpClient shuffleClient;
  private final byte[] buffer;

  PartialSegmentMergeTask(
      // id shouldn't be null except when this task is created by ParallelIndexSupervisorTask
      @Nullable String id,
      final String groupId,
      final TaskResource taskResource,
      final String supervisorTaskId,
      DataSchema dataSchema,
      PartialSegmentMergeIOConfig<P> ioConfig,
      ParallelIndexTuningConfig tuningConfig,
      final int numAttempts, // zero-based counting
      final Map<String, Object> context,
      IndexingServiceClient indexingServiceClient,
      IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> taskClientFactory,
      HttpClient shuffleClient
  )
  {
    super(
        id,
        groupId,
        taskResource,
        dataSchema,
        tuningConfig,
        context
    );

    this.ioConfig = ioConfig;
    this.numAttempts = numAttempts;
    this.supervisorTaskId = supervisorTaskId;
    this.indexingServiceClient = indexingServiceClient;
    this.taskClientFactory = taskClientFactory;
    this.shuffleClient = shuffleClient;
    this.buffer = new byte[BUFFER_SIZE];
  }

  @JsonProperty
  public int getNumAttempts()
  {
    return numAttempts;
  }

  @JsonProperty
  public String getSupervisorTaskId()
  {
    return supervisorTaskId;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient)
  {
    return true;
  }

  @Override
  public TaskStatus runTask(TaskToolbox toolbox) throws Exception
  {
    // Group partitionLocations by interval and partitionId
    final Map<Interval, Int2ObjectMap<List<P>>> intervalToPartitions = new HashMap<>();
    for (P location : ioConfig.getPartitionLocations()) {
      intervalToPartitions.computeIfAbsent(location.getInterval(), k -> new Int2ObjectOpenHashMap<>())
                         .computeIfAbsent(location.getPartitionId(), k -> new ArrayList<>())
                         .add(location);
    }

    final List<TaskLock> locks = toolbox.getTaskActionClient().submit(
        new SurrogateAction<>(supervisorTaskId, new LockListAction())
    );
    final Map<Interval, String> intervalToVersion = Maps.newHashMapWithExpectedSize(locks.size());
    locks.forEach(lock -> {
      if (lock.isRevoked()) {
        throw new ISE("Lock[%s] is revoked", lock);
      }
      final String mustBeNull = intervalToVersion.put(lock.getInterval(), lock.getVersion());
      if (mustBeNull != null) {
        throw new ISE(
            "WTH? Two versions([%s], [%s]) for the same interval[%s]?",
            lock.getVersion(),
            mustBeNull,
            lock.getInterval()
        );
      }
    });

    final Stopwatch fetchStopwatch = Stopwatch.createStarted();
    final Map<Interval, Int2ObjectMap<List<File>>> intervalToUnzippedFiles = fetchSegmentFiles(
        toolbox,
        intervalToPartitions
    );
    final long fetchTime = fetchStopwatch.elapsed(TimeUnit.SECONDS);
    fetchStopwatch.stop();
    LOG.info("Fetch took [%s] seconds", fetchTime);

    final ParallelIndexSupervisorTaskClient taskClient = taskClientFactory.build(
        new ClientBasedTaskInfoProvider(indexingServiceClient),
        getId(),
        1, // always use a single http thread
        getTuningConfig().getChatHandlerTimeout(),
        getTuningConfig().getChatHandlerNumRetries()
    );

    final File persistDir = toolbox.getPersistDir();
    FileUtils.deleteQuietly(persistDir);
    FileUtils.forceMkdir(persistDir);

    final Set<DataSegment> pushedSegments = mergeAndPushSegments(
        toolbox,
        getDataSchema(),
        getTuningConfig(),
        persistDir,
        intervalToVersion,
        intervalToUnzippedFiles
    );

    taskClient.report(supervisorTaskId, new PushedSegmentsReport(getId(), Collections.emptySet(), pushedSegments));

    return TaskStatus.success(getId());
  }

  private Map<Interval, Int2ObjectMap<List<File>>> fetchSegmentFiles(
      TaskToolbox toolbox,
      Map<Interval, Int2ObjectMap<List<P>>> intervalToPartitions
  ) throws IOException
  {
    final File tempDir = toolbox.getIndexingTmpDir();
    FileUtils.deleteQuietly(tempDir);
    FileUtils.forceMkdir(tempDir);

    final Map<Interval, Int2ObjectMap<List<File>>> intervalToUnzippedFiles = new HashMap<>();
    // Fetch partition files
    for (Entry<Interval, Int2ObjectMap<List<P>>> entryPerInterval : intervalToPartitions.entrySet()) {
      final Interval interval = entryPerInterval.getKey();
      for (Int2ObjectMap.Entry<List<P>> entryPerPartitionId :
          entryPerInterval.getValue().int2ObjectEntrySet()) {
        final int partitionId = entryPerPartitionId.getIntKey();
        final File partitionDir = FileUtils.getFile(
            tempDir,
            interval.getStart().toString(),
            interval.getEnd().toString(),
            Integer.toString(partitionId)
        );
        FileUtils.forceMkdir(partitionDir);
        for (P location : entryPerPartitionId.getValue()) {
          final File zippedFile = fetchSegmentFile(partitionDir, location);
          try {
            final File unzippedDir = new File(partitionDir, StringUtils.format("unzipped_%s", location.getSubTaskId()));
            FileUtils.forceMkdir(unzippedDir);
            CompressionUtils.unzip(zippedFile, unzippedDir);
            intervalToUnzippedFiles.computeIfAbsent(interval, k -> new Int2ObjectOpenHashMap<>())
                                   .computeIfAbsent(partitionId, k -> new ArrayList<>())
                                   .add(unzippedDir);
          }
          finally {
            if (!zippedFile.delete()) {
              LOG.warn("Failed to delete temp file[%s]", zippedFile);
            }
          }
        }
      }
    }
    return intervalToUnzippedFiles;
  }

  @VisibleForTesting
  File fetchSegmentFile(File partitionDir, P location) throws IOException
  {
    final File zippedFile = new File(partitionDir, StringUtils.format("temp_%s", location.getSubTaskId()));
    final URI uri = location.toIntermediaryDataServerURI(supervisorTaskId);
    org.apache.druid.java.util.common.FileUtils.copyLarge(
        uri,
        u -> {
          try {
            return shuffleClient.go(new Request(HttpMethod.GET, u.toURL()), new InputStreamResponseHandler())
                                .get();
          }
          catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        },
        zippedFile,
        buffer,
        t -> t instanceof IOException,
        NUM_FETCH_RETRIES,
        StringUtils.format("Failed to fetch file[%s]", uri)
    );
    return zippedFile;
  }

  /**
   * Create a {@link ShardSpec} suitable for the desired secondary partitioning strategy.
   */
  abstract S createShardSpec(TaskToolbox toolbox, Interval interval, int partitionId);

  private Set<DataSegment> mergeAndPushSegments(
      TaskToolbox toolbox,
      DataSchema dataSchema,
      ParallelIndexTuningConfig tuningConfig,
      File persistDir,
      Map<Interval, String> intervalToVersion,
      Map<Interval, Int2ObjectMap<List<File>>> intervalToUnzippedFiles
  ) throws Exception
  {
    final DataSegmentPusher segmentPusher = toolbox.getSegmentPusher();
    final Set<DataSegment> pushedSegments = new HashSet<>();
    for (Entry<Interval, Int2ObjectMap<List<File>>> entryPerInterval : intervalToUnzippedFiles.entrySet()) {
      final Interval interval = entryPerInterval.getKey();
      for (Int2ObjectMap.Entry<List<File>> entryPerPartitionId : entryPerInterval.getValue().int2ObjectEntrySet()) {
        final int partitionId = entryPerPartitionId.getIntKey();
        final List<File> segmentFilesToMerge = entryPerPartitionId.getValue();
        final Pair<File, List<String>> mergedFileAndDimensionNames = mergeSegmentsInSamePartition(
            dataSchema,
            tuningConfig,
            toolbox.getIndexIO(),
            toolbox.getIndexMergerV9(),
            segmentFilesToMerge,
            tuningConfig.getMaxNumSegmentsToMerge(),
            persistDir,
            0
        );
        final List<String> metricNames = Arrays.stream(dataSchema.getAggregators())
                                               .map(AggregatorFactory::getName)
                                               .collect(Collectors.toList());

        // Retry pushing segments because uploading to deep storage might fail especially for cloud storage types
        final DataSegment segment = RetryUtils.retry(
            () -> segmentPusher.push(
                mergedFileAndDimensionNames.lhs,
                new DataSegment(
                    getDataSource(),
                    interval,
                    Preconditions.checkNotNull(intervalToVersion.get(interval), "version for interval[%s]", interval),
                    null, // will be filled in the segmentPusher
                    mergedFileAndDimensionNames.rhs,
                    metricNames,
                    createShardSpec(toolbox, interval, partitionId),
                    null, // will be filled in the segmentPusher
                    0     // will be filled in the segmentPusher
                ),
                false
            ),
            exception -> exception instanceof Exception,
            5
        );
        pushedSegments.add(segment);
      }
    }
    return pushedSegments;
  }

  private static Pair<File, List<String>> mergeSegmentsInSamePartition(
      DataSchema dataSchema,
      ParallelIndexTuningConfig tuningConfig,
      IndexIO indexIO,
      IndexMergerV9 merger,
      List<File> indexes,
      int maxNumSegmentsToMerge,
      File baseOutDir,
      int outDirSuffix
  ) throws IOException
  {
    int suffix = outDirSuffix;
    final List<File> mergedFiles = new ArrayList<>();
    List<String> dimensionNames = null;
    for (int i = 0; i < indexes.size(); i += maxNumSegmentsToMerge) {
      final List<File> filesToMerge = indexes.subList(i, Math.min(i + maxNumSegmentsToMerge, indexes.size()));
      final List<QueryableIndex> indexesToMerge = new ArrayList<>(filesToMerge.size());
      final Closer indexCleaner = Closer.create();
      for (File file : filesToMerge) {
        final QueryableIndex queryableIndex = indexIO.loadIndex(file);
        indexesToMerge.add(queryableIndex);
        indexCleaner.register(() -> {
          queryableIndex.close();
          file.delete();
        });
      }
      if (maxNumSegmentsToMerge >= indexes.size()) {
        dimensionNames = IndexMerger.getMergedDimensionsFromQueryableIndexes(indexesToMerge);
      }
      final File outDir = new File(baseOutDir, StringUtils.format("merged_%d", suffix++));
      mergedFiles.add(
          merger.mergeQueryableIndex(
              indexesToMerge,
              dataSchema.getGranularitySpec().isRollup(),
              dataSchema.getAggregators(),
              outDir,
              tuningConfig.getIndexSpec(),
              tuningConfig.getSegmentWriteOutMediumFactory()
          )
      );

      indexCleaner.close();
    }

    if (mergedFiles.size() == 1) {
      return Pair.of(mergedFiles.get(0), Preconditions.checkNotNull(dimensionNames, "dimensionNames"));
    } else {
      return mergeSegmentsInSamePartition(
          dataSchema,
          tuningConfig,
          indexIO,
          merger,
          mergedFiles,
          maxNumSegmentsToMerge,
          baseOutDir,
          suffix
      );
    }
  }
}
