package com.metamx.druid.indexer.path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.Granularity;
import com.metamx.common.guava.Comparators;
import com.metamx.druid.common.s3.S3Utils;
import com.metamx.druid.indexer.HadoopDruidIndexerConfig;
import com.metamx.druid.indexer.granularity.UniformGranularitySpec;
import com.metamx.druid.indexer.hadoop.FSSpideringIterator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.codehaus.jackson.annotate.JsonProperty;
import org.jets3t.service.model.S3Object;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;
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
        String.format("Cannot use %s without %s", GranularUnprocessedPathSpec.class.getSimpleName(), UniformGranularitySpec.class.getSimpleName())
    );

    final Path betaInput = new Path(getInputPath());
    final FileSystem fs = betaInput.getFileSystem(job.getConfiguration());
    final Granularity segmentGranularity = ((UniformGranularitySpec)config.getGranularitySpec()).getGranularity();

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

      String bucketOutput = String.format("%s/%s", config.getSegmentOutputDir(), segmentGranularity.toPath(timeBucket));
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

    config.setGranularitySpec(new UniformGranularitySpec(segmentGranularity, Lists.newArrayList(bucketsToRun)));

    return super.addInputPaths(config, job);
  }
}