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

package io.druid.indexer.path;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.Granularity;
import com.metamx.common.guava.Comparators;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.hadoop.FSSpideringIterator;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 */
public class GranularUnprocessedPathSpec extends GranularityPathSpec
{
  private int maxBuckets;

  @JsonProperty
  public int getMaxBuckets()
  {
    return maxBuckets;
  }

  public void setMaxBuckets(int maxBuckets)
  {
    this.maxBuckets = maxBuckets;
  }

  @Override
  public Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException
  {
    // This PathSpec breaks so many abstractions that we might as break some more
    Preconditions.checkState(
        config.getGranularitySpec() instanceof UniformGranularitySpec,
        String.format(
            "Cannot use %s without %s",
            GranularUnprocessedPathSpec.class.getSimpleName(),
            UniformGranularitySpec.class.getSimpleName()
        )
    );

    final Path betaInput = new Path(getInputPath());
    final FileSystem fs = betaInput.getFileSystem(job.getConfiguration());
    final Granularity segmentGranularity = ((UniformGranularitySpec) config.getGranularitySpec()).getSegmentGranularity();

    Map<DateTime, Long> inputModifiedTimes = new TreeMap<DateTime, Long>(
        Comparators.inverse(Comparators.<Comparable>comparable())
    );

    for (FileStatus status : FSSpideringIterator.spiderIterable(fs, betaInput)) {
      final DateTime key = segmentGranularity.toDate(status.getPath().toString());
      final Long currVal = inputModifiedTimes.get(key);
      final long mTime = status.getModificationTime();

      inputModifiedTimes.put(key, currVal == null ? mTime : Math.max(currVal, mTime));
    }

    Set<Interval> bucketsToRun = Sets.newTreeSet(Comparators.intervals());
    for (Map.Entry<DateTime, Long> entry : inputModifiedTimes.entrySet()) {
      DateTime timeBucket = entry.getKey();
      long mTime = entry.getValue();

      String bucketOutput = String.format(
          "%s/%s",
          config.getSpec().getIOConfig().getSegmentOutputPath(),
          segmentGranularity.toPath(timeBucket)
      );
      for (FileStatus fileStatus : FSSpideringIterator.spiderIterable(fs, new Path(bucketOutput))) {
        if (fileStatus.getModificationTime() > mTime) {
          bucketsToRun.add(new Interval(timeBucket, segmentGranularity.increment(timeBucket)));
          break;
        }
      }

      if (bucketsToRun.size() >= maxBuckets) {
        break;
      }
    }

    config.setGranularitySpec(
        new UniformGranularitySpec(
            segmentGranularity,
            config.getGranularitySpec().getQueryGranularity(),
            Lists.newArrayList(bucketsToRun),
            segmentGranularity
        )
    );

    return super.addInputPaths(config, job);
  }
}
