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

package com.metamx.druid.merger.common.index;

import java.io.File;
import java.util.List;
import java.util.Set;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeName;
import org.codehaus.jackson.map.annotate.JacksonInject;
import org.joda.time.Interval;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import com.metamx.druid.Query;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.druid.index.v1.IndexMerger;
import com.metamx.druid.index.v1.MMappedIndex;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.realtime.FireDepartmentMetrics;
import com.metamx.druid.realtime.FireHydrant;
import com.metamx.druid.realtime.Plumber;
import com.metamx.druid.realtime.PlumberSchool;
import com.metamx.druid.realtime.Schema;
import com.metamx.druid.realtime.SegmentPusher;
import com.metamx.druid.realtime.Sink;

/**
 * Trains plumbers that create a single historical segment.
 */
@JsonTypeName("historical")
public class YeOldePlumberSchool implements PlumberSchool
{
  private final Interval interval;
  private final String version;
  private final SegmentPusher segmentPusher;
  private final File tmpSegmentDir;

  private static final Logger log = new Logger(YeOldePlumberSchool.class);

  @JsonCreator
  public YeOldePlumberSchool(
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      @JacksonInject("segmentPusher") SegmentPusher segmentPusher,
      @JacksonInject("tmpSegmentDir") File tmpSegmentDir
  )
  {
    this.interval = interval;
    this.version = version;
    this.segmentPusher = segmentPusher;
    this.tmpSegmentDir = tmpSegmentDir;
  }

  @Override
  public Plumber findPlumber(final Schema schema, final FireDepartmentMetrics metrics)
  {
    // There can be only one.
    final Sink theSink = new Sink(interval, schema);

    final File persistDir = new File(
        tmpSegmentDir, theSink.getSegment().withVersion(version).getIdentifier()
    );

    final Set<File> spilled = Sets.newHashSet();

    return new Plumber()
    {
      @Override
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
        try {
          // User should have persisted everything by now.
          Preconditions.checkState(!theSink.swappable(), "All data must be persisted before fininshing the job!");

          // The segment we will upload
          final File fileToUpload;

          if(spilled.size() == 0) {
            throw new IllegalStateException("Nothing indexed?");
          } else if(spilled.size() == 1) {
            fileToUpload = Iterables.getOnlyElement(spilled);
          } else {
            List<MMappedIndex> indexes = Lists.newArrayList();
            for (final File oneSpill : spilled) {
              indexes.add(IndexIO.mapDir(oneSpill));
            }

            fileToUpload = new File(tmpSegmentDir, "merged");
            IndexMerger.mergeMMapped(indexes, schema.getAggregators(), fileToUpload);
          }

          final DataSegment segmentToUpload = theSink.getSegment().withVersion(version);
          segmentPusher.push(fileToUpload, segmentToUpload);

          log.info(
              "Uploaded segment[%s]",
              segmentToUpload.getIdentifier()
          );

        } catch(Exception e) {
          log.warn(e, "Failed to merge and upload");
          throw Throwables.propagate(e);
        }
      }

      private void spillIfSwappable()
      {
        if(theSink.swappable()) {
          final FireHydrant indexToPersist = theSink.swap();
          final int rowsToPersist = indexToPersist.getIndex().size();
          final File dirToPersist = getSpillDir(indexToPersist.getCount());

          log.info("Spilling index[%d] with rows[%d] to: %s", indexToPersist.getCount(), rowsToPersist, dirToPersist);

          try {

            IndexMerger.persist(
                indexToPersist.getIndex(),
                dirToPersist
            );

            indexToPersist.swapAdapter(null);

            metrics.incrementRowOutputCount(rowsToPersist);

            spilled.add(dirToPersist);

          } catch(Exception e) {
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
