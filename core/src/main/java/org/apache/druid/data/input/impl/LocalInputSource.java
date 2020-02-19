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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputFileAttribute;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.utils.Streams;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class LocalInputSource extends AbstractInputSource implements SplittableInputSource<List<File>>
{
  private final File baseDir;
  private final String filter;

  @JsonCreator
  public LocalInputSource(
      @JsonProperty("baseDir") File baseDir,
      @JsonProperty("filter") String filter
  )
  {
    this.baseDir = Preconditions.checkNotNull(baseDir, "baseDir");
    this.filter = Preconditions.checkNotNull(filter, "filter");
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
  public Stream<InputSplit<List<File>>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    return Streams.sequentialStreamFrom(getSplitFileIterator(getSplitHintSpecOrDefault(splitHintSpec)))
                  .map(InputSplit::new);
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    return Iterators.size(getSplitFileIterator(getSplitHintSpecOrDefault(splitHintSpec)));
  }

  private Iterator<List<File>> getSplitFileIterator(SplitHintSpec splitHintSpec)
  {
    final Iterator<File> fileIterator = getFileIterator();
    return splitHintSpec.split(fileIterator, file -> new InputFileAttribute(file.length()));
  }

  @VisibleForTesting
  Iterator<File> getFileIterator()
  {
    return FileUtils.iterateFiles(
        baseDir.getAbsoluteFile(),
        new WildcardFileFilter(filter),
        TrueFileFilter.INSTANCE
    );
  }

  @Override
  public SplittableInputSource<List<File>> withSplit(InputSplit<List<File>> split)
  {
    return new SpecificFilesLocalInputSource(split.get());
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
    //noinspection ConstantConditions
    return new InputEntityIteratingReader(
        inputRowSchema,
        inputFormat,
        Iterators.transform(getFileIterator(), FileEntity::new),
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
    LocalInputSource that = (LocalInputSource) o;
    return Objects.equals(baseDir, that.baseDir) &&
           Objects.equals(filter, that.filter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(baseDir, filter);
  }
}
