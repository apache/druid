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

package io.druid.indexing.common.index;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.QueryableIndex;
import io.druid.segment.SegmentUtils;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.TuningConfigs;
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
  private final IndexMergerV9 indexMergerV9;
  private final IndexIO indexIO;

  private static final Logger log = new Logger(YeOldePlumberSchool.class);

  @JsonCreator
  public YeOldePlumberSchool(
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      @JacksonInject("segmentPusher") DataSegmentPusher dataSegmentPusher,
      @JacksonInject("tmpSegmentDir") File tmpSegmentDir,
      @JacksonInject IndexMergerV9 indexMergerV9,
      @JacksonInject IndexIO indexIO
  )
  {
    this.interval = interval;
    this.version = version;
    this.dataSegmentPusher = dataSegmentPusher;
    this.tmpSegmentDir = tmpSegmentDir;
    this.indexMergerV9 = Preconditions.checkNotNull(indexMergerV9, "Null IndexMergerV9");
    this.indexIO = Preconditions.checkNotNull(indexIO, "Null IndexIO");
  }

  @Override
  public Plumber findPlumber(
      final DataSchema schema,
      final RealtimeTuningConfig config,
      final FireDepartmentMetrics metrics
  )
  {
    // There can be only one.
    final Sink theSink = new Sink(
        interval,
        schema,
        config.getShardSpec(),
        version,
        config.getMaxRowsInMemory(),
        TuningConfigs.getMaxBytesInMemoryOrDefault(config.getMaxBytesInMemory()),
        config.isReportParseExceptions()
    );

    // Temporary directory to hold spilled segments.
    final File persistDir = new File(tmpSegmentDir, theSink.getSegment().getIdentifier());

    // Set of spilled segments. Will be merged at the end.
    final Set<File> spilled = Sets.newHashSet();

    return new Plumber()
    {
      @Override
      public Object startJob()
      {
        return null;
      }

      @Override
      public int add(InputRow row, Supplier<Committer> committerSupplier) throws IndexSizeExceededException
      {
        Sink sink = getSink(row.getTimestampFromEpoch());
        if (sink == null) {
          return -1;
        }

        final int numRows = sink.add(row, false).getRowCount();

        if (!sink.canAppendRow()) {
          persist(committerSupplier.get());
        }

        return numRows;
      }

      private Sink getSink(long timestamp)
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
      public void persist(Committer committer)
      {
        spillIfSwappable();
        committer.run();
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
              indexes.add(indexIO.loadIndex(oneSpill));
            }

            fileToUpload = new File(tmpSegmentDir, "merged");
            indexMergerV9.mergeQueryableIndex(
                indexes,
                schema.getGranularitySpec().isRollup(),
                schema.getAggregators(),
                fileToUpload,
                config.getIndexSpec(),
                config.getSegmentWriteOutMediumFactory()
            );
          }

          // Map merged segment so we can extract dimensions
          final QueryableIndex mappedSegment = indexIO.loadIndex(fileToUpload);

          final DataSegment segmentToUpload = theSink.getSegment()
                                                     .withDimensions(ImmutableList.copyOf(mappedSegment.getAvailableDimensions()))
                                                     .withBinaryVersion(SegmentUtils.getVersionFromDir(fileToUpload));

          dataSegmentPusher.push(fileToUpload, segmentToUpload, false);

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
            indexMergerV9.persist(
                indexToPersist.getIndex(),
                dirToPersist,
                config.getIndexSpec(),
                config.getSegmentWriteOutMediumFactory()
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
        return new File(persistDir, StringUtils.format("spill%d", n));
      }
    };
  }
}
