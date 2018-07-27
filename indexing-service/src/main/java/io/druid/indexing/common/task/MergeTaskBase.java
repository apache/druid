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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentListUsedAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.emitter.service.ServiceMetricEvent;
import io.druid.segment.IndexIO;
import io.druid.segment.writeout.SegmentWriteOutMediumFactory;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public abstract class MergeTaskBase extends AbstractFixedIntervalTask
{
  private static final EmittingLogger log = new EmittingLogger(MergeTaskBase.class);

  @JsonIgnore
  private final List<DataSegment> segments;
  @JsonIgnore
  @Nullable
  private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;

  protected MergeTaskBase(
      final String id,
      final String dataSource,
      final List<DataSegment> segments,
      final @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      Map<String, Object> context
  )
  {
    super(
        // _not_ the version, just something uniqueish
        id != null ? id : StringUtils.format(
            "merge_%s_%s", computeProcessingID(dataSource, segments), DateTimes.nowUtc().toString()
        ),
        dataSource,
        computeMergedInterval(segments),
        context
    );

    // Verify segment list is nonempty
    Preconditions.checkArgument(segments.size() > 0, "segments nonempty");
    // Verify segments are all in the correct datasource
    Preconditions.checkArgument(
        Iterables.size(
            Iterables.filter(
                segments,
                new Predicate<DataSegment>()
                {
                  @Override
                  public boolean apply(@Nullable DataSegment segment)
                  {
                    return segment == null || !segment.getDataSource().equalsIgnoreCase(dataSource);
                  }
                }
            )
        ) == 0, "segments in the wrong datasource"
    );
    verifyInputSegments(segments);

    this.segments = segments;
    this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
  }

  protected void verifyInputSegments(List<DataSegment> segments)
  {
    // Verify segments are all unsharded
    Preconditions.checkArgument(
        Iterables.size(
            Iterables.filter(
                segments,
                new Predicate<DataSegment>()
                {
                  @Override
                  public boolean apply(@Nullable DataSegment segment)
                  {
                    return segment == null || !(segment.getShardSpec() instanceof NoneShardSpec);
                  }
                }
            )
        ) == 0, "segments without NoneShardSpec"
    );
  }

  @Override
  public int getDefaultPriority()
  {
    return Tasks.DEFAULT_MERGE_TASK_PRIORITY;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    final TaskLock myLock = Iterables.getOnlyElement(getTaskLocks(toolbox.getTaskActionClient()));
    final ServiceEmitter emitter = toolbox.getEmitter();
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    final DataSegment mergedSegment = computeMergedSegment(getDataSource(), myLock.getVersion(), segments);
    final File mergeDir = toolbox.getMergeDir();

    try {
      final long startTime = System.currentTimeMillis();

      log.info(
          "Starting merge of id[%s], segments: %s",
          getId(),
          Lists.transform(
              segments,
              new Function<DataSegment, String>()
              {
                @Override
                public String apply(DataSegment input)
                {
                  return input.getIdentifier();
                }
              }
          )
      );

      // download segments to merge
      final Map<DataSegment, File> gettedSegments = toolbox.fetchSegments(segments);

      // merge files together
      final File fileToUpload = merge(toolbox, gettedSegments, mergeDir);

      emitter.emit(builder.build("merger/numMerged", segments.size()));
      emitter.emit(builder.build("merger/mergeTime", System.currentTimeMillis() - startTime));

      log.info(
          "[%s] : Merged %d segments in %,d millis",
          mergedSegment.getDataSource(),
          segments.size(),
          System.currentTimeMillis() - startTime
      );

      long uploadStart = System.currentTimeMillis();

      // Upload file

      final DataSegment uploadedSegment = toolbox.getSegmentPusher().push(fileToUpload, mergedSegment, false);

      emitter.emit(builder.build("merger/uploadTime", System.currentTimeMillis() - uploadStart));
      emitter.emit(builder.build("merger/mergeSize", uploadedSegment.getSize()));

      toolbox.publishSegments(ImmutableList.of(uploadedSegment));

      return TaskStatus.success(getId());
    }
    catch (Exception e) {
      log.makeAlert(e, "Exception merging[%s]", mergedSegment.getDataSource())
         .addData("interval", mergedSegment.getInterval())
         .emit();

      return TaskStatus.failure(getId());
    }
  }

  /**
   * Checks pre-existing segments in "context" to confirm that this merge query is valid. Specifically, confirm that
   * we are operating on every segment that overlaps the chosen interval.
   */
  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    // Try to acquire lock
    if (!super.isReady(taskActionClient)) {
      return false;
    } else {
      final Function<DataSegment, String> toIdentifier = new Function<DataSegment, String>()
      {
        @Override
        public String apply(DataSegment dataSegment)
        {
          return dataSegment.getIdentifier();
        }
      };

      final Set<String> current = ImmutableSet.copyOf(
          Iterables.transform(
              taskActionClient.submit(new SegmentListUsedAction(getDataSource(), getInterval(), null)),
              toIdentifier
          )
      );
      final Set<String> requested = ImmutableSet.copyOf(Iterables.transform(segments, toIdentifier));

      final Set<String> missingFromRequested = Sets.difference(current, requested);
      if (!missingFromRequested.isEmpty()) {
        throw new ISE(
            "Merge is invalid: current segment(s) are not in the requested set: %s",
            Joiner.on(", ").join(missingFromRequested)
        );
      }

      final Set<String> missingFromCurrent = Sets.difference(requested, current);
      if (!missingFromCurrent.isEmpty()) {
        throw new ISE(
            "Merge is invalid: requested segment(s) are not in the current set: %s",
            Joiner.on(", ").join(missingFromCurrent)
        );
      }

      return true;
    }
  }

  protected abstract File merge(TaskToolbox taskToolbox, Map<DataSegment, File> segments, File outDir)
      throws Exception;

  @JsonProperty
  public List<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty
  @Nullable
  public SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory()
  {
    return segmentWriteOutMediumFactory;
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("id", getId())
                  .add("dataSource", getDataSource())
                  .add("interval", getInterval())
                  .add("segments", segments)
                  .add("segmentWriteOutMediumFactory", segmentWriteOutMediumFactory)
                  .toString();
  }

  private static String computeProcessingID(final String dataSource, final List<DataSegment> segments)
  {
    final String segmentIDs = Joiner.on("_").join(
        Iterables.transform(
            Ordering.natural().sortedCopy(segments), new Function<DataSegment, String>()
            {
              @Override
              public String apply(DataSegment x)
              {
                return StringUtils.format(
                    "%s_%s_%s_%s",
                    x.getInterval().getStart(),
                    x.getInterval().getEnd(),
                    x.getVersion(),
                    x.getShardSpec().getPartitionNum()
                );
              }
            }
        )
    );

    return StringUtils.format(
        "%s_%s",
        dataSource,
        Hashing.sha1().hashString(segmentIDs, Charsets.UTF_8).toString()
    );
  }

  private static Interval computeMergedInterval(final List<DataSegment> segments)
  {
    Preconditions.checkArgument(segments.size() > 0, "segments.size() > 0");

    DateTime start = null;
    DateTime end = null;

    for (final DataSegment segment : segments) {
      if (start == null || segment.getInterval().getStart().isBefore(start)) {
        start = segment.getInterval().getStart();
      }

      if (end == null || segment.getInterval().getEnd().isAfter(end)) {
        end = segment.getInterval().getEnd();
      }
    }

    return new Interval(start, end);
  }

  private static DataSegment computeMergedSegment(
      final String dataSource,
      final String version,
      final List<DataSegment> segments
  )
  {
    final Interval mergedInterval = computeMergedInterval(segments);
    final Set<String> mergedDimensions = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
    final Set<String> mergedMetrics = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

    for (DataSegment segment : segments) {
      mergedDimensions.addAll(segment.getDimensions());
      mergedMetrics.addAll(segment.getMetrics());
    }

    return DataSegment.builder()
                      .dataSource(dataSource)
                      .interval(mergedInterval)
                      .version(version)
                      .binaryVersion(IndexIO.CURRENT_VERSION_ID)
                      .shardSpec(NoneShardSpec.instance())
                      .dimensions(Lists.newArrayList(mergedDimensions))
                      .metrics(Lists.newArrayList(mergedMetrics))
                      .build();
  }
}
