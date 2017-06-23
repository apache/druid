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

package io.druid.indexer.path;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.hadoop.FSSpideringIterator;
import io.druid.java.util.common.guava.Comparators;
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
        StringUtils.format(
            "Cannot use %s without %s",
            GranularUnprocessedPathSpec.class.getSimpleName(),
            UniformGranularitySpec.class.getSimpleName()
        )
    );

    final Path betaInput = new Path(getInputPath());
    final FileSystem fs = betaInput.getFileSystem(job.getConfiguration());
    final Granularity segmentGranularity = config.getGranularitySpec().getSegmentGranularity();

    Map<Long, Long> inputModifiedTimes = new TreeMap<>(Ordering.natural().reverse());

    for (FileStatus status : FSSpideringIterator.spiderIterable(fs, betaInput)) {
      final DateTime key = segmentGranularity.toDate(status.getPath().toString());
      final Long currVal = inputModifiedTimes.get(key.getMillis());
      final long mTime = status.getModificationTime();

      inputModifiedTimes.put(key.getMillis(), currVal == null ? mTime : Math.max(currVal, mTime));
    }

    Set<Interval> bucketsToRun = Sets.newTreeSet(Comparators.intervals());
    for (Map.Entry<Long, Long> entry : inputModifiedTimes.entrySet()) {
      DateTime timeBucket = new DateTime(entry.getKey());
      long mTime = entry.getValue();

      String bucketOutput = StringUtils.format(
          "%s/%s",
          config.getSchema().getIOConfig().getSegmentOutputPath(),
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
            config.getGranularitySpec().isRollup(),
            Lists.newArrayList(bucketsToRun)

        )
    );

    return super.addInputPaths(config, job);
  }
}
