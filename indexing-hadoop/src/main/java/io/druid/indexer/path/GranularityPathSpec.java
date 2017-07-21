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
import com.google.common.collect.Sets;

import io.druid.java.util.common.granularity.Granularity;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.hadoop.FSSpideringIterator;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.common.logger.Logger;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
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
  private Class<? extends InputFormat> inputFormat;

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
  public Class<? extends InputFormat> getInputFormat()
  {
    return inputFormat;
  }

  public void setInputFormat(Class<? extends InputFormat> inputFormat)
  {
    this.inputFormat = inputFormat;
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
    final Set<Interval> intervals = Sets.newTreeSet(Comparators.intervalsByStartThenEnd());
    for (Interval inputInterval : config.getInputIntervals()) {
      for (Interval interval : dataGranularity.getIterable(inputInterval)) {
        intervals.add(trim(inputInterval, interval));
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
      String intervalPath;
      if (customFormatter != null) {
        intervalPath = customFormatter.print(t);
      } else {
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

    log.info("Appending path %s", paths);
    StaticPathSpec.addToMultipleInputs(config, job, paths, inputFormat);

    return job;
  }

  private Interval trim(Interval inputInterval, Interval interval)
  {
    long start = interval.getStartMillis();
    long end = interval.getEndMillis();

    boolean makeNew = false;
    if (start < inputInterval.getStartMillis()) {
      start = inputInterval.getStartMillis();
      makeNew = true;
    }
    if (end > inputInterval.getEndMillis()) {
      end = inputInterval.getEndMillis();
      makeNew = true;
    }
    return makeNew ? new Interval(start, end) : interval;
  }

}
