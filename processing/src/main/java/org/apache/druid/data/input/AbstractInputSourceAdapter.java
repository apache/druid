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

package org.apache.druid.data.input;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.data.input.impl.LocalInputSourceAdapter;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * A wrapper on top of {@link SplittableInputSource} that handles input source creation.
 * For composing input sources such as IcebergInputSource, the delegate input source instantiation might fail upon deserialization since the input file paths
 * are not available yet and this might fail the input source precondition checks.
 * This adapter helps create the delegate input source once the input file paths are fully determined.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = LocalInputSourceAdapter.TYPE_KEY, value = LocalInputSourceAdapter.class)
})
public abstract class AbstractInputSourceAdapter
{
  private SplittableInputSource inputSource;

  public abstract SplittableInputSource generateInputSource(List<String> inputFilePaths);

  public void setupInputSource(List<String> inputFilePaths)
  {
    if (inputSource != null) {
      throw new ISE("Inputsource is already initialized!");
    }
    if (inputFilePaths.isEmpty()) {
      inputSource = new EmptyInputSource();
    } else {
      inputSource = generateInputSource(inputFilePaths);
    }
  }

  public SplittableInputSource getInputSource()
  {
    if (inputSource == null) {
      throw new ISE("Inputsource is not initialized yet!");
    }
    return inputSource;
  }

  private static class EmptyInputSource implements SplittableInputSource
  {
    @Override
    public boolean needsFormat()
    {
      return false;
    }

    @Override
    public boolean isSplittable()
    {
      return false;
    }

    @Override
    public InputSourceReader reader(
        InputRowSchema inputRowSchema,
        @Nullable InputFormat inputFormat,
        File temporaryDirectory
    )
    {
      return new InputSourceReader()
      {
        @Override
        public CloseableIterator<InputRow> read(InputStats inputStats)
        {
          return CloseableIterators.wrap(Collections.emptyIterator(), () -> {
          });
        }

        @Override
        public CloseableIterator<InputRowListPlusRawValues> sample()
        {
          return CloseableIterators.wrap(Collections.emptyIterator(), () -> {
          });
        }
      };
    }

    @Override
    public Stream<InputSplit> createSplits(
        InputFormat inputFormat,
        @Nullable SplitHintSpec splitHintSpec
    ) throws IOException
    {
      return Stream.empty();
    }

    @Override
    public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec) throws IOException
    {
      return 0;
    }

    @Override
    public InputSource withSplit(InputSplit split)
    {
      return null;
    }
  }
}
