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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LocalInputSource extends AbstractInputSource implements SplittableInputSource<File>
{
  private final File baseDir;
  private final String filter;

  @JsonCreator
  public LocalInputSource(
      @JsonProperty("baseDir") File baseDir,
      @JsonProperty("filter") String filter
  )
  {
    this.baseDir = baseDir;
    this.filter = filter;
  }

  @JsonProperty
  public File getBaseDir()
  {
    return baseDir;
  }

  @JsonProperty
  public String getFilter()
  {
    return filter;
  }

  @Override
  public Stream<InputSplit<File>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(getFileIterator(), Spliterator.DISTINCT), false)
                        .map(InputSplit::new);
  }

  @Override
  public int getNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    return Iterators.size(getFileIterator());
  }

  private Iterator<File> getFileIterator()
  {
    return FileUtils.iterateFiles(
        Preconditions.checkNotNull(baseDir).getAbsoluteFile(),
        new WildcardFileFilter(filter),
        TrueFileFilter.INSTANCE
    );
  }

  @Override
  public SplittableInputSource<File> withSplit(InputSplit<File> split)
  {
    final File file = split.get();
    return new LocalInputSource(file.getParentFile(), file.getName());
  }

  @Override
  public boolean needsFormat()
  {
    return true;
  }

  @Override
  protected InputSourceReader formattableReader(
      InputRowSchema inputRowSchema,
      InputFormat inputFormat,
      @Nullable File temporaryDirectory
  )
  {
    return new InputEntityIteratingReader<>(
        inputRowSchema,
        inputFormat,
        // reader() is supposed to be called in each task that creates segments.
        // The task should already have only one split in parallel indexing,
        // while there's no need to make splits using splitHintSpec in sequential indexing.
        createSplits(inputFormat, null).map(split -> new FileSource(split.get())),
        temporaryDirectory
    );
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
    LocalInputSource source = (LocalInputSource) o;
    return Objects.equals(baseDir, source.baseDir) &&
           Objects.equals(filter, source.filter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(baseDir, filter);
  }
}
