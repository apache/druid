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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.java.util.common.logger.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.Set;


public class StaticPathSpec implements PathSpec
{
  private static final Logger log = new Logger(StaticPathSpec.class);

  private final String paths;
  private final Class<? extends InputFormat> inputFormat;

  @JsonCreator
  public StaticPathSpec(
      @JsonProperty("paths") String paths,
      @JsonProperty("inputFormat") Class<? extends InputFormat> inputFormat
  )
  {
    this.paths = paths;
    this.inputFormat = inputFormat;
  }

  @Override
  public Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException
  {
    log.info("Adding paths[%s]", paths);

    addToMultipleInputs(config, job, paths, inputFormat);

    return job;
  }

  @JsonProperty
  public Class<? extends InputFormat> getInputFormat()
  {
    return inputFormat;
  }

  @JsonProperty
  public String getPaths()
  {
    return paths;
  }

  public static void addToMultipleInputs(
      HadoopDruidIndexerConfig config,
      Job job,
      String path,
      Class<? extends InputFormat> inputFormatClass
  )
  {
    if (path != null) {
      addToMultipleInputs(config, job, ImmutableSet.of(path), inputFormatClass);
    }
  }

  public static void addToMultipleInputs(
      HadoopDruidIndexerConfig config,
      Job job,
      Set<String> paths,
      Class<? extends InputFormat> inputFormatClass
  )
  {
    if (paths == null || paths.isEmpty()) {
      return;
    }

    Class<? extends InputFormat> inputFormatClassToUse = inputFormatClass;
    if (inputFormatClassToUse == null) {
      if (config.isCombineText()) {
        inputFormatClassToUse = CombineTextInputFormat.class;
      } else {
        inputFormatClassToUse = TextInputFormat.class;
      }
    }

    // Due to https://issues.apache.org/jira/browse/MAPREDUCE-5061 we can't directly do
    // MultipleInputs.addInputPath(job, path, inputFormatClassToUse)
    // but have to handle hadoop glob path ourselves correctly
    // This change and HadoopGlobPathSplitter.java can be removed once the hadoop issue is fixed
    Set<String> pathStrings = Sets.newLinkedHashSet();
    for (String path : paths) {
      Iterables.addAll(pathStrings, HadoopGlobPathSplitter.splitGlob(path));
    }
    if (!pathStrings.isEmpty()) {
      addInputPath(job, pathStrings, inputFormatClassToUse);
    }
  }

  // copied from MultipleInputs.addInputPath with slight modifications
  private static void addInputPath(Job job, Iterable<String> pathStrings, Class<? extends InputFormat> inputFormatClass)
  {
    Configuration conf = job.getConfiguration();
    StringBuilder inputFormats = new StringBuilder(Strings.nullToEmpty(conf.get(MultipleInputs.DIR_FORMATS)));

    String[] paths = Iterables.toArray(pathStrings, String.class);
    for (int i = 0; i < paths.length - 1; i++) {
      if (inputFormats.length() > 0) {
        inputFormats.append(',');
      }
      inputFormats.append(paths[i]).append(';').append(inputFormatClass.getName());
    }
    if (inputFormats.length() > 0) {
      conf.set(MultipleInputs.DIR_FORMATS, inputFormats.toString());
    }
    // add last one separately for possible initialization in MultipleInputs
    MultipleInputs.addInputPath(job, new Path(paths[paths.length - 1]), inputFormatClass);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StaticPathSpec that = (StaticPathSpec) o;

    if (paths != null ? !paths.equals(that.paths) : that.paths != null) {
      return false;
    }
    return !(inputFormat != null ? !inputFormat.equals(that.inputFormat) : that.inputFormat != null);

  }

  @Override
  public int hashCode()
  {
    int result = paths != null ? paths.hashCode() : 0;
    result = 31 * result + (inputFormat != null ? inputFormat.hashCode() : 0);
    return result;
  }
}
