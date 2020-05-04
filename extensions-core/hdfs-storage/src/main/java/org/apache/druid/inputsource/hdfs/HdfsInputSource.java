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

package org.apache.druid.inputsource.hdfs;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputFileAttribute;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.InputEntityIteratingReader;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.guice.Hdfs;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.utils.Streams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HdfsInputSource extends AbstractInputSource implements SplittableInputSource<List<Path>>
{
  private static final String PROP_PATHS = "paths";

  private final List<String> inputPaths;
  private final Configuration configuration;

  // Although the javadocs for SplittableInputSource say to avoid caching splits to reduce memory, HdfsInputSource
  // *does* cache the splits for the following reasons:
  //
  // 1) It will improve compatibility with the index_hadoop task, allowing people to easily migrate from Hadoop.
  //    For example, input paths with globs will be supported (lazily expanding the wildcard glob is tricky).
  //
  // 2) The index_hadoop task allocates splits eagerly, so the memory usage should not be a problem for anyone
  //    migrating from Hadoop.
  private List<Path> cachedPaths;

  @JsonCreator
  public HdfsInputSource(
      @JsonProperty(PROP_PATHS) Object inputPaths,
      @JacksonInject @Hdfs Configuration configuration
  )
  {
    this.inputPaths = coerceInputPathsToList(inputPaths, PROP_PATHS);
    this.configuration = configuration;
    this.cachedPaths = null;
  }

  public static List<String> coerceInputPathsToList(Object inputPaths, String propertyName)
  {
    final List<String> paths;

    if (inputPaths instanceof String) {
      paths = Collections.singletonList((String) inputPaths);
    } else if (inputPaths instanceof List && ((List<?>) inputPaths).stream().allMatch(x -> x instanceof String)) {
      paths = ((List<?>) inputPaths).stream().map(x -> (String) x).collect(Collectors.toList());
    } else {
      throw new IAE("'%s' must be a string or an array of strings", propertyName);
    }

    return paths;
  }

  public static Collection<Path> getPaths(List<String> inputPaths, Configuration configuration) throws IOException
  {
    if (inputPaths.isEmpty()) {
      return Collections.emptySet();
    }

    // Use FileInputFormat to read splits. To do this, we need to make a fake Job.
    Job job = Job.getInstance(configuration);

    // Add paths to the fake JobContext.
    for (String inputPath : inputPaths) {
      FileInputFormat.addInputPaths(job, inputPath);
    }

    return new HdfsFileInputFormat().getSplits(job)
                                    .stream()
                                    .filter(split -> ((FileSplit) split).getLength() > 0)
                                    .map(split -> ((FileSplit) split).getPath())
                                    .collect(Collectors.toSet());
  }

  /**
   * Helper for leveraging hadoop code to interpret HDFS paths with globs
   */
  private static class HdfsFileInputFormat extends FileInputFormat<Object, Object>
  {
    @Override
    public RecordReader<Object, Object> createRecordReader(
        org.apache.hadoop.mapreduce.InputSplit inputSplit,
        TaskAttemptContext taskAttemptContext
    )
    {
      throw new UnsupportedOperationException();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename)
    {
      return false;  // prevent generating extra paths
    }
  }

  @VisibleForTesting
  @JsonProperty(PROP_PATHS)
  List<String> getInputPaths()
  {
    return inputPaths;
  }

  @Override
  protected InputSourceReader formattableReader(
      InputRowSchema inputRowSchema,
      InputFormat inputFormat,
      @Nullable File temporaryDirectory
  )
  {
    try {
      cachePathsIfNeeded();
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return new InputEntityIteratingReader(
        inputRowSchema,
        inputFormat,
        Iterators.transform(cachedPaths.iterator(), path -> new HdfsInputEntity(configuration, path)),
        temporaryDirectory
    );
  }

  @Override
  public Stream<InputSplit<List<Path>>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
      throws IOException
  {
    cachePathsIfNeeded();
    final Iterator<List<Path>> splitIterator = getSplitHintSpecOrDefault(splitHintSpec).split(
        cachedPaths.iterator(),
        path -> {
          try {
            final long size = path.getFileSystem(configuration).getFileStatus(path).getLen();
            return new InputFileAttribute(size);
          }
          catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
    );
    return Streams.sequentialStreamFrom(splitIterator).map(InputSplit::new);
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec) throws IOException
  {
    cachePathsIfNeeded();
    return cachedPaths.size();
  }

  @Override
  public SplittableInputSource<List<Path>> withSplit(InputSplit<List<Path>> split)
  {
    List<String> paths = split.get().stream().map(path -> path.toString()).collect(Collectors.toList());
    return new HdfsInputSource(paths, configuration);
  }

  @Override
  public boolean needsFormat()
  {
    return true;
  }

  private void cachePathsIfNeeded() throws IOException
  {
    if (cachedPaths == null) {
      cachedPaths = ImmutableList.copyOf(Preconditions.checkNotNull(getPaths(inputPaths, configuration), "paths"));
    }
  }

  static Builder builder()
  {
    return new Builder();
  }

  static final class Builder
  {
    private Object paths;
    private Configuration configuration;

    private Builder()
    {
    }

    Builder paths(Object paths)
    {
      this.paths = paths;
      return this;
    }

    Builder configuration(Configuration configuration)
    {
      this.configuration = configuration;
      return this;
    }

    HdfsInputSource build()
    {
      return new HdfsInputSource(paths, configuration);
    }
  }
}
