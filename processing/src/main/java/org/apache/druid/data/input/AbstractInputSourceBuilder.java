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
import org.apache.druid.data.input.impl.LocalInputSourceBuilder;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.java.util.common.CloseableIterators;
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
    @JsonSubTypes.Type(name = LocalInputSourceBuilder.TYPE_KEY, value = LocalInputSourceBuilder.class)
})
public abstract class AbstractInputSourceBuilder
{
  public abstract SplittableInputSource generateInputSource(List<String> inputFilePaths);

  public SplittableInputSource setupInputSource(List<String> inputFilePaths)
  {
    if (inputFilePaths.isEmpty()) {
      return new EmptyInputSource();
    } else {
      return generateInputSource(inputFilePaths);
    }
  }

  /*
   * This input source is used in place of a delegate input source if there are no input file paths.
   * Certain input sources cannot be instantiated with an empty input file list and so composing input sources such as IcebergInputSource
   * may use this input source as delegate in such cases.
   */
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
