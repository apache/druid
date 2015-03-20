/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexer.path;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.logger.Logger;
import io.druid.indexer.HadoopDruidIndexerConfig;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Class uses public fields to work around http://jira.codehaus.org/browse/MSHADE-92
 *
 * Adjust to JsonCreator and final fields when resolved.
 */
public class StaticPathSpec implements PathSpec
{
  private static final Logger log = new Logger(StaticPathSpec.class);

  @JsonProperty("paths")
  public String paths;

  @JsonProperty("inputFormat")
  private final Class<? extends InputFormat> inputFormat;

  public StaticPathSpec()
  {
    this(null, null);
  }

  public StaticPathSpec(
      String paths,
      Class<? extends InputFormat> inputFormat
  )
  {
    this.paths = paths;
    this.inputFormat = inputFormat;
  }

  @Override
  public Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException
  {
    log.info("Adding paths[%s]", paths);
    FileInputFormat.addInputPaths(job, paths);
    return job;
  }

  public Class<? extends InputFormat> getInputFormat()
  {
    return inputFormat;
  }
}
