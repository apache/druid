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
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.InputEntityIteratingReader;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class HdfsInputSource extends AbstractInputSource implements SplittableInputSource<Path>
{
  private static final String PROP_PATHS = "paths";

  private final List<String> inputPaths;
  private final Configuration configuration;

  @JsonCreator
  public HdfsInputSource(
      @JsonProperty(PROP_PATHS) Object inputPaths,
      @JacksonInject Configuration configuration
  )
  {
    // Note: Currently commas or globs in InputPaths are not supported
    this.inputPaths = coerceInputPathsToList(inputPaths);

    this.configuration = configuration;
  }

  private static List<String> coerceInputPathsToList(Object inputPaths)
  {
    final List<String> paths;

    if (inputPaths instanceof String) {
      paths = Collections.singletonList((String) inputPaths);
    } else if (inputPaths instanceof List && ((List<?>) inputPaths).stream().allMatch(x -> x instanceof String)) {
      paths = ((List<?>) inputPaths).stream().map(x -> (String) x).collect(Collectors.toList());
    } else {
      throw new IAE("'%s' must be a string or an array of strings", PROP_PATHS);
    }

    return paths;
  }


  @JsonProperty(PROP_PATHS)
  private List<String> getInputPaths()
  {
    return inputPaths;
  }

  @Override
  protected InputSourceReader formattableReader(
      InputRowSchema inputRowSchema,
      @Nullable InputFormat inputFormat,
      @Nullable File temporaryDirectory
  )
  {
    return new InputEntityIteratingReader(
        inputRowSchema,
        inputFormat,
        createSplits(null, null).map(split -> new HdfsSource(configuration, split.get())),
        temporaryDirectory
    );
  }

  @Override
  public Stream<InputSplit<Path>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    return inputPaths.stream()
                     .map(Path::new)
                     .flatMap(path -> {
                       try {
                         FileSystem fs = path.getFileSystem(configuration);
                         RemoteIterator<FileStatus> delegate = fs.listStatusIterator(fs.makeQualified(path));
                         final Iterator<FileStatus> fileIterator = new FileStatusIterator(delegate);
                         return StreamSupport.stream(
                             Spliterators.spliteratorUnknownSize(
                                 fileIterator,
                                 Spliterator.DISTINCT & Spliterator.NONNULL
                             ),
                             false
                         );
                       }
                       catch (IOException e) {
                         throw new UncheckedIOException(e);
                       }
                     })
                     .map(fileStatus -> new InputSplit<>(fileStatus.getPath()));
  }

  @Override
  public int getNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec) throws IOException
  {
    int numSplits = 0;
    for (String inputPath : inputPaths) {
      Path path = new Path(inputPath);
      FileSystem fs = path.getFileSystem(configuration);
      final RemoteIterator<FileStatus> iterator = fs.listStatusIterator(path);
      numSplits += size(iterator);
    }

    return numSplits;
  }

  private static int size(RemoteIterator<FileStatus> remoteIterator) throws IOException
  {
    int size = 0;
    while (remoteIterator.hasNext()) {
      remoteIterator.next();
      size++;
    }
    return size;
  }

  @Override
  public SplittableInputSource<Path> withSplit(InputSplit<Path> split)
  {
    return new HdfsInputSource(split.get().toString(), configuration);
  }

  @Override
  public boolean needsFormat()
  {
    return true;
  }

  private static class FileStatusIterator implements Iterator<FileStatus>
  {
    RemoteIterator<FileStatus> delegate;

    FileStatusIterator(RemoteIterator<FileStatus> remoteIterator)
    {
      delegate = remoteIterator;
    }

    @Override
    public boolean hasNext()
    {
      try {
        return delegate.hasNext();
      }
      catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public FileStatus next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      try {
        return delegate.next();
      }
      catch (IOException e) {
        throw new UncheckedIOException(e);
      }
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
