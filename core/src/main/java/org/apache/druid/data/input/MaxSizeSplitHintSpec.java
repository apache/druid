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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;

/**
 * A SplitHintSpec that can create splits of multiple files.
 * A split created by this class can have one or more input files.
 * If there is only one file in the split, its size can be larger than {@link #maxSplitSize}.
 * If there are two or more files in the split, their total size cannot be larger than {@link #maxSplitSize}.
 */
public class MaxSizeSplitHintSpec implements SplitHintSpec
{
  public static final String TYPE = "maxSize";

  @VisibleForTesting
  static final long DEFAULT_MAX_SPLIT_SIZE = 512 * 1024 * 1024;

  private final long maxSplitSize;

  @JsonCreator
  public MaxSizeSplitHintSpec(@JsonProperty("maxSplitSize") @Nullable Long maxSplitSize)
  {
    this.maxSplitSize = maxSplitSize == null ? DEFAULT_MAX_SPLIT_SIZE : maxSplitSize;
  }

  @JsonProperty
  public long getMaxSplitSize()
  {
    return maxSplitSize;
  }

  @Override
  public <T> Iterator<List<T>> split(Iterator<T> inputIterator, Function<T, InputFileAttribute> inputAttributeExtractor)
  {
    final Iterator<T> nonEmptyFileOnlyIterator = Iterators.filter(
        inputIterator,
        input -> inputAttributeExtractor.apply(input).getSize() > 0
    );
    return new Iterator<List<T>>()
    {
      private T peeking;

      @Override
      public boolean hasNext()
      {
        return peeking != null || nonEmptyFileOnlyIterator.hasNext();
      }

      @Override
      public List<T> next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final List<T> current = new ArrayList<>();
        long splitSize = 0;
        while (splitSize < maxSplitSize && (peeking != null || nonEmptyFileOnlyIterator.hasNext())) {
          if (peeking == null) {
            peeking = nonEmptyFileOnlyIterator.next();
          }
          final long size = inputAttributeExtractor.apply(peeking).getSize();
          if (current.isEmpty() || splitSize + size < maxSplitSize) {
            current.add(peeking);
            splitSize += size;
            peeking = null;
          } else {
            break;
          }
        }
        assert !current.isEmpty();
        return current;
      }
    };
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
    MaxSizeSplitHintSpec that = (MaxSizeSplitHintSpec) o;
    return maxSplitSize == that.maxSplitSize;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(maxSplitSize);
  }
}
