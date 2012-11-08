/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.merger.common.task;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.loading.SegmentGetter;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.coordinator.TaskContext;
import com.metamx.druid.shard.NoneShardSpec;
import com.metamx.emitter.service.AlertEvent;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.apache.commons.codec.digest.DigestUtils;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = DefaultMergeTask.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "append", value = AppendTask.class)
})
public abstract class MergeTask extends AbstractTask
{
  private final List<DataSegment> segments;

  private static final Logger log = new Logger(MergeTask.class);

  protected MergeTask(final String dataSource, final List<DataSegment> segments)
  {
    super(
        // _not_ the version, just something uniqueish
        String.format("merge_%s_%s", computeProcessingID(dataSource, segments), new DateTime().toString()),
        dataSource,
        computeMergedInterval(segments)
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

    this.segments = segments;
  }

  @Override
  public TaskStatus run(TaskContext context, TaskToolbox toolbox) throws Exception
  {
    final ServiceEmitter emitter = toolbox.getEmitter();
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    final DataSegment mergedSegment = computeMergedSegment(getDataSource(), context.getVersion(), segments);
    final File taskDir = toolbox.getConfig().getTaskDir(this);

    try {

      final long startTime = System.currentTimeMillis();

      log.info(
          "Starting merge of id[%s], segments: %s", getId(), Lists.transform(
          segments,
          new Function<DataSegment, String>()
          {
            @Override
            public String apply(@Nullable DataSegment input)
            {
              return input.getIdentifier();
            }
          }
      )
      );


      // download segments to merge
      final Map<String, SegmentGetter> segmentGetters = toolbox.getSegmentGetters(this);
      final Map<DataSegment, File> gettedSegments = Maps.newHashMap();
      for (final DataSegment segment : segments) {
        Map<String, Object> loadSpec = segment.getLoadSpec();
        SegmentGetter segmentGetter = segmentGetters.get(loadSpec.get("type"));
        gettedSegments.put(segment, segmentGetter.getSegmentFiles(loadSpec));
      }

      // merge files together
      final File fileToUpload = merge(gettedSegments, new File(taskDir, "merged"));

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
      final DataSegment uploadedSegment = toolbox.getSegmentPusher().push(fileToUpload, mergedSegment);

      emitter.emit(builder.build("merger/uploadTime", System.currentTimeMillis() - uploadStart));
      emitter.emit(builder.build("merger/mergeSize", uploadedSegment.getSize()));

      return TaskStatus.success(getId(), Lists.newArrayList(uploadedSegment));
    }
    catch (Exception e) {
      log.error(
          e,
          String.format(
              "Exception merging %s[%s] segments",
              mergedSegment.getDataSource(),
              mergedSegment.getInterval()
          )
      );
      emitter.emit(
          new AlertEvent.Builder().build(
              "Exception merging",
              ImmutableMap.<String, Object>builder()
                          .put("exception", e.toString())
                          .build()
          )
      );

      return TaskStatus.failure(getId());
    }
  }

  /**
   * Checks pre-existing segments in "context" to confirm that this merge query is valid. Specifically, confirm that
   * we are operating on every segment that overlaps the chosen interval.
   */
  @Override
  public TaskStatus preflight(TaskContext context)
  {
    final Function<DataSegment, String> toIdentifier = new Function<DataSegment, String>()
    {
      @Override
      public String apply(DataSegment dataSegment)
      {
        return dataSegment.getIdentifier();
      }
    };

    final Set<String> current = ImmutableSet.copyOf(Iterables.transform(context.getCurrentSegments(), toIdentifier));
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

    return TaskStatus.running(getId());

  }

  protected abstract File merge(Map<DataSegment, File> segments, File outDir)
      throws Exception;

  @JsonProperty
  public List<DataSegment> getSegments()
  {
    return segments;
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("id", getId())
                  .add("dataSource", getDataSource())
                  .add("interval", getInterval())
                  .add("segments", segments)
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
            return String.format(
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

    return String.format("%s_%s", dataSource, DigestUtils.shaHex(segmentIDs).toLowerCase());
  }

  private static Interval computeMergedInterval(final List<DataSegment> segments)
  {
    Preconditions.checkArgument(segments.size() > 0, "segments.size() > 0");

    DateTime start = null;
    DateTime end = null;

    for(final DataSegment segment : segments) {
      if(start == null || segment.getInterval().getStart().isBefore(start)) {
        start = segment.getInterval().getStart();
      }

      if(end == null || segment.getInterval().getEnd().isAfter(end)) {
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
                      .shardSpec(new NoneShardSpec())
                      .dimensions(Lists.newArrayList(mergedDimensions))
                      .metrics(Lists.newArrayList(mergedMetrics))
                      .build();
  }
}
