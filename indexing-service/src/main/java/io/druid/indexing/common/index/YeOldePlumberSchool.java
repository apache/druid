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

package io.druid.indexing.common.index;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import io.druid.data.input.InputRow;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMaker;
import io.druid.segment.QueryableIndex;
import io.druid.segment.SegmentUtils;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.FireHydrant;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.segment.realtime.plumber.PlumberSchool;
import io.druid.segment.realtime.plumber.Sink;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Trains plumbers that create a single historical segment.
 */
@JsonTypeName("historical")
public class YeOldePlumberSchool implements PlumberSchool
{
  private final Interval interval;
  private final String version;
  private final DataSegmentPusher dataSegmentPusher;
  private final File tmpSegmentDir;

  private static final Logger log = new Logger(YeOldePlumberSchool.class);

  @JsonCreator
  public YeOldePlumberSchool(
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      @JacksonInject("segmentPusher") DataSegmentPusher dataSegmentPusher,
      @JacksonInject("tmpSegmentDir") File tmpSegmentDir
  )
  {
    this.interval = interval;
    this.version = version;
    this.dataSegmentPusher = dataSegmentPusher;
    this.tmpSegmentDir = tmpSegmentDir;
  }

  @Override
  public Plumber findPlumber(
      final DataSchema schema,
      final RealtimeTuningConfig config,
      final FireDepartmentMetrics metrics
  )
  {
    // There can be only one.
    final Sink theSink = new Sink(interval, schema, config, version);

    // Temporary directory to hold spilled segments.
    final File persistDir = new File(tmpSegmentDir, theSink.getSegment().getIdentifier());

    // Set of spilled segments. Will be merged at the end.
    final Set<File> spilled = Sets.newHashSet();

    return new Plumber()
    {
      @Override
      public void startJob()
      {

      }

      @Override
      public int add(InputRow row) throws IndexSizeExceededException
      {
        Sink sink = getSink(row.getTimestampFromEpoch());
        if (sink == null) {
          return -1;
        }

        return sink.add(row);
      }

      public Sink getSink(long timestamp)
      {
        if (theSink.getInterval().contains(timestamp)) {
          return theSink;
        } else {
          return null;
        }
      }

      @Override
      public <T> QueryRunner<T> getQueryRunner(Query<T> query)
      {
        throw new UnsupportedOperationException("Don't query me, bro.");
      }

      @Override
      public void persist(Runnable commitRunnable)
      {
        spillIfSwappable();
        commitRunnable.run();
      }

      @Override
      public void finishJob()
      {
        // The segment we will upload
        File fileToUpload = null;

        try {
          // User should have persisted everything by now.
          Preconditions.checkState(!theSink.swappable(), "All data must be persisted before fininshing the job!");

          if (spilled.size() == 0) {
            throw new IllegalStateException("Nothing indexed?");
          } else if (spilled.size() == 1) {
            fileToUpload = Iterables.getOnlyElement(spilled);
          } else {
            List<QueryableIndex> indexes = Lists.newArrayList();
            for (final File oneSpill : spilled) {
              indexes.add(IndexIO.loadIndex(oneSpill));
            }

            fileToUpload = new File(tmpSegmentDir, "merged");
            IndexMaker.mergeQueryableIndex(indexes, schema.getAggregators(), fileToUpload);
          }

          // Map merged segment so we can extract dimensions
          final QueryableIndex mappedSegment = IndexIO.loadIndex(fileToUpload);

          final DataSegment segmentToUpload = theSink.getSegment()
                                                     .withDimensions(ImmutableList.copyOf(mappedSegment.getAvailableDimensions()))
                                                     .withBinaryVersion(SegmentUtils.getVersionFromDir(fileToUpload));

          dataSegmentPusher.push(fileToUpload, segmentToUpload);

          log.info(
              "Uploaded segment[%s]",
              segmentToUpload.getIdentifier()
          );

        }
        catch (Exception e) {
          log.warn(e, "Failed to merge and upload");
          throw Throwables.propagate(e);
        }
        finally {
          try {
            if (fileToUpload != null) {
              log.info("Deleting Index File[%s]", fileToUpload);
              FileUtils.deleteDirectory(fileToUpload);
            }
          }
          catch (IOException e) {
            log.warn(e, "Error deleting directory[%s]", fileToUpload);
          }
        }
      }

      private void spillIfSwappable()
      {
        if (theSink.swappable()) {
          final FireHydrant indexToPersist = theSink.swap();
          final int rowsToPersist = indexToPersist.getIndex().size();
          final File dirToPersist = getSpillDir(indexToPersist.getCount());

          log.info("Spilling index[%d] with rows[%d] to: %s", indexToPersist.getCount(), rowsToPersist, dirToPersist);

          try {
            IndexMaker.persist(
                indexToPersist.getIndex(),
                dirToPersist
            );

            indexToPersist.swapSegment(null);

            metrics.incrementRowOutputCount(rowsToPersist);

            spilled.add(dirToPersist);

          }
          catch (Exception e) {
            log.warn(e, "Failed to spill index[%d]", indexToPersist.getCount());
            throw Throwables.propagate(e);
          }
        }
      }

      private File getSpillDir(final int n)
      {
        return new File(persistDir, String.format("spill%d", n));
      }
    };
  }
}
