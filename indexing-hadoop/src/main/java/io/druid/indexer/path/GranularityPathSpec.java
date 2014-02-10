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
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.metamx.common.Granularity;
import com.metamx.common.guava.Comparators;
import com.metamx.common.logger.Logger;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.hadoop.FSSpideringIterator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.Set;
import java.util.regex.Pattern;

/**
 */
public class GranularityPathSpec implements PathSpec
{
  private static final Logger log = new Logger(GranularityPathSpec.class);

  private String inputPath;
  private String filePattern;
  private Granularity dataGranularity;
  private String pathFormat;

  @JsonProperty
  public String getInputPath()
  {
    return inputPath;
  }

  public void setInputPath(String inputPath)
  {
    this.inputPath = inputPath;
  }

  @JsonProperty
  public String getFilePattern()
  {
    return filePattern;
  }

  public void setFilePattern(String filePattern)
  {
    this.filePattern = filePattern;
  }

  @JsonProperty
  public Granularity getDataGranularity()
  {
    return dataGranularity;
  }

  public void setDataGranularity(Granularity dataGranularity)
  {
    this.dataGranularity = dataGranularity;
  }

  @JsonProperty
  public String getPathFormat()
  {
    return pathFormat;
  }

  public void setPathFormat(String pathFormat)
  {
    this.pathFormat = pathFormat;
  }

  @Override
  public Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException
  {
    final Set<Interval> intervals = Sets.newTreeSet(Comparators.intervals());
    Optional<Set<Interval>> optionalIntervals = config.getSegmentGranularIntervals();
    if (optionalIntervals.isPresent()) {
      for (Interval segmentInterval : optionalIntervals.get()) {
        for (Interval dataInterval : dataGranularity.getIterable(segmentInterval)) {
          intervals.add(dataInterval);
        }
      }
    }

    Path betaInput = new Path(inputPath);
    FileSystem fs = betaInput.getFileSystem(job.getConfiguration());
    Set<String> paths = Sets.newTreeSet();
    Pattern fileMatcher = Pattern.compile(filePattern);

    DateTimeFormatter customFormatter = null;
    if(pathFormat != null) {
      customFormatter = DateTimeFormat.forPattern(pathFormat);
    }

    for (Interval interval : intervals) {
      DateTime t = interval.getStart();
      String intervalPath = null;
      if(customFormatter != null) {
        intervalPath = customFormatter.print(t);
      }
      else {
        intervalPath = dataGranularity.toPath(t);
      }

      Path granularPath = new Path(betaInput, intervalPath);
      log.info("Checking path[%s]", granularPath);
      for (FileStatus status : FSSpideringIterator.spiderIterable(fs, granularPath)) {
        final Path filePath = status.getPath();
        if (fileMatcher.matcher(filePath.toString()).matches()) {
          paths.add(filePath.toString());
        }
      }
    }

    for (String path : paths) {
      log.info("Appending path[%s]", path);
      FileInputFormat.addInputPath(job, new Path(path));
    }

    return job;
  }
}
