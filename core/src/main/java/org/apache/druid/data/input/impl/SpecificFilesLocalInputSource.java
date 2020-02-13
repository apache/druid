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
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputAttribute;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.utils.CollectionUtils;
import org.apache.druid.utils.Streams;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class SpecificFilesLocalInputSource extends AbstractInputSource implements SplittableInputSource<List<File>>
{
  private final List<File> files;

  @JsonCreator
  public SpecificFilesLocalInputSource(@JsonProperty("files") List<File> files)
  {
    Preconditions.checkArgument(!CollectionUtils.isNullOrEmpty(files), "empty files");
    this.files = files;
  }

  @JsonProperty
  public List<File> getFiles()
  {
    return files;
  }

  @Override
  public Stream<InputSplit<List<File>>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    final Iterator<List<File>> iterator = getSplitHintSpecOrDefault(splitHintSpec).split(
        files.iterator(),
        file -> new InputAttribute(file.length())
    );
    return Streams.sequentialStreamFrom(iterator).map(InputSplit::new);
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    final Iterator<List<File>> iterator = getSplitHintSpecOrDefault(splitHintSpec).split(
        files.iterator(),
        file -> new InputAttribute(file.length())
    );
    return Iterators.size(iterator);
  }

  @Override
  public InputSource withSplit(InputSplit<List<File>> split)
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
        Iterators.transform(files.iterator(), FileEntity::new),
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
    SpecificFilesLocalInputSource that = (SpecificFilesLocalInputSource) o;
    return Objects.equals(files, that.files);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(files);
  }

  @Override
  public String toString()
  {
    return "SpecificFilesLocalInputSource{" +
           "files=" + files +
           '}';
  }
}
