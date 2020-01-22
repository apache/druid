/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.firehose.hdfs;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.prefetch.PrefetchableTextFilesFirehoseFactory;
import org.apache.druid.guice.Hdfs;
import org.apache.druid.inputsource.hdfs.HdfsInputSource;
import org.apache.druid.storage.hdfs.HdfsDataSegmentPuller;
import org.apache.druid.utils.CompressionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;

public class HdfsFirehoseFactory extends PrefetchableTextFilesFirehoseFactory<Path>
{
  private final List<String> inputPaths;
  private final Configuration conf;

  @JsonCreator
  public HdfsFirehoseFactory(
      @JacksonInject @Hdfs Configuration conf,
      @JsonProperty("paths") Object inputPaths,
      @JsonProperty("maxCacheCapacityBytes") Long maxCacheCapacityBytes,
      @JsonProperty("maxFetchCapacityBytes") Long maxFetchCapacityBytes,
      @JsonProperty("prefetchTriggerBytes") Long prefetchTriggerBytes,
      @JsonProperty("fetchTimeout") Long fetchTimeout,
      @JsonProperty("maxFetchRetry") Integer maxFetchRetry
  )
  {
    super(maxCacheCapacityBytes, maxFetchCapacityBytes, prefetchTriggerBytes, fetchTimeout, maxFetchRetry);
    this.inputPaths = HdfsInputSource.coerceInputPathsToList(inputPaths, "inputPaths");
    this.conf = conf;
  }

  @JsonProperty("paths")
  public List<String> getInputPaths()
  {
    return inputPaths;
  }

  @Override
  protected Collection<Path> initObjects() throws IOException
  {
    return HdfsInputSource.getPaths(inputPaths, conf);
  }

  @Override
  protected InputStream openObjectStream(Path path) throws IOException
  {
    return path.getFileSystem(conf).open(path);
  }

  @Override
  protected InputStream openObjectStream(Path path, long start) throws IOException
  {
    final FSDataInputStream in = path.getFileSystem(conf).open(path);
    in.seek(start);
    return in;
  }

  @Override
  protected InputStream wrapObjectStream(Path path, InputStream stream) throws IOException
  {
    return CompressionUtils.decompress(stream, path.getName());
  }

  @Override
  protected Predicate<Throwable> getRetryCondition()
  {
    return HdfsDataSegmentPuller.RETRY_PREDICATE;
  }

  @Override
  public boolean isSplittable()
  {
    return true;
  }

  @Override
  public FiniteFirehoseFactory<StringInputRowParser, Path> withSplit(InputSplit<Path> split)
  {
    return new HdfsFirehoseFactory(
        conf,
        split.get().toString(),
        getMaxCacheCapacityBytes(),
        getMaxFetchCapacityBytes(),
        getPrefetchTriggerBytes(),
        getFetchTimeout(),
        getMaxFetchRetry()
    );
  }

  @Override
  public String toString()
  {
    return "HdfsFirehoseFactory{" +
           "inputPaths=" + inputPaths +
           '}';
  }
}
