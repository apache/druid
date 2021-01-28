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
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.SurrogateAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.ClientBasedTaskInfoProvider;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
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
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Base class for creating task that merges partial segments created by {@link PartialSegmentGenerateTask}.
 */
abstract class PartialSegmentMergeTask<S extends ShardSpec, P extends PartitionLocation> extends PerfectRollupWorkerTask
{
  private static final Logger LOG = new Logger(PartialSegmentMergeTask.class);


  private final PartialSegmentMergeIOConfig<P> ioConfig;
  private final int numAttempts;
  private final String supervisorTaskId;

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
      final Map<String, Object> context
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

    Preconditions.checkArgument(
        !dataSchema.getGranularitySpec().inputIntervals().isEmpty(),
        "Missing intervals in granularitySpec"
    );
    this.ioConfig = ioConfig;
    this.numAttempts = numAttempts;
    this.supervisorTaskId = supervisorTaskId;
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
    final Map<Interval, Int2ObjectMap<List<P>>> intervalToBuckets = new HashMap<>();
    for (P location : ioConfig.getPartitionLocations()) {
      intervalToBuckets.computeIfAbsent(location.getInterval(), k -> new Int2ObjectOpenHashMap<>())
                         .computeIfAbsent(location.getBucketId(), k -> new ArrayList<>())
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
            "Unexpected state: Two versions([%s], [%s]) for the same interval[%s]",
            lock.getVersion(),
            mustBeNull,
            lock.getInterval()
        );
      }
    });

    final Stopwatch fetchStopwatch = Stopwatch.createStarted();
    final Map<Interval, Int2ObjectMap<List<File>>> intervalToUnzippedFiles = fetchSegmentFiles(
        toolbox,
        intervalToBuckets
    );
    final long fetchTime = fetchStopwatch.elapsed(TimeUnit.SECONDS);
    fetchStopwatch.stop();
    LOG.info("Fetch took [%s] seconds", fetchTime);

    final ParallelIndexSupervisorTaskClient taskClient = toolbox.getSupervisorTaskClientFactory().build(
        new ClientBasedTaskInfoProvider(toolbox.getIndexingServiceClient()),
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
      Map<Interval, Int2ObjectMap<List<P>>> intervalToBuckets
  ) throws IOException
  {
    final File tempDir = toolbox.getIndexingTmpDir();
    FileUtils.deleteQuietly(tempDir);
    FileUtils.forceMkdir(tempDir);

    final Map<Interval, Int2ObjectMap<List<File>>> intervalToUnzippedFiles = new HashMap<>();
    // Fetch partition files
    for (Entry<Interval, Int2ObjectMap<List<P>>> entryPerInterval : intervalToBuckets.entrySet()) {
      final Interval interval = entryPerInterval.getKey();
      for (Int2ObjectMap.Entry<List<P>> entryPerBucketId :
          entryPerInterval.getValue().int2ObjectEntrySet()) {
        final int bucketId = entryPerBucketId.getIntKey();
        final File partitionDir = FileUtils.getFile(
            tempDir,
            interval.getStart().toString(),
            interval.getEnd().toString(),
            Integer.toString(bucketId)
        );
        FileUtils.forceMkdir(partitionDir);
        for (P location : entryPerBucketId.getValue()) {
          final File zippedFile = toolbox.getShuffleClient().fetchSegmentFile(partitionDir, supervisorTaskId, location);
          try {
            final File unzippedDir = new File(partitionDir, StringUtils.format("unzipped_%s", location.getSubTaskId()));
            FileUtils.forceMkdir(unzippedDir);
            CompressionUtils.unzip(zippedFile, unzippedDir);
            intervalToUnzippedFiles.computeIfAbsent(interval, k -> new Int2ObjectOpenHashMap<>())
                                   .computeIfAbsent(bucketId, k -> new ArrayList<>())
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

  /**
   * Create a {@link ShardSpec} suitable for the desired secondary partitioning strategy.
   */
  abstract S createShardSpec(TaskToolbox toolbox, Interval interval, int bucketId);

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
      for (Int2ObjectMap.Entry<List<File>> entryPerBucketId : entryPerInterval.getValue().int2ObjectEntrySet()) {
        final int bucketId = entryPerBucketId.getIntKey();
        final List<File> segmentFilesToMerge = entryPerBucketId.getValue();
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
                    Preconditions.checkNotNull(
                        ParallelIndexSupervisorTask.findVersion(intervalToVersion, interval),
                        "version for interval[%s]",
                        interval
                    ),
                    null, // will be filled in the segmentPusher
                    mergedFileAndDimensionNames.rhs,
                    metricNames,
                    createShardSpec(toolbox, interval, bucketId),
                    null, // will be filled in the segmentPusher
                    0     // will be filled in the segmentPusher
                ),
                false
            ),
            exception -> !(exception instanceof NullPointerException) && exception instanceof Exception,
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
              tuningConfig.getSegmentWriteOutMediumFactory(),
              tuningConfig.getMaxColumnsToMerge()
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
