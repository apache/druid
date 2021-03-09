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
import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.java.util.common.Pair;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * InputSource that combines data from multiple inputSources. The delegate inputSources must be splittable.
 * The splits for this inputSource are created from the {@link SplittableInputSource#createSplits} of the delegate inputSources.
 * Each inputSplit is paired up with its respective delegate inputSource so that during split,
 * {@link SplittableInputSource#withSplit}is called against the correct inputSource for each inputSplit.
 * This inputSource presently only supports a single {@link InputFormat}.
 */

public class CombiningInputSource extends AbstractInputSource implements SplittableInputSource
{
  private final List<SplittableInputSource> delegates;

  @JsonCreator
  public CombiningInputSource(
      @JsonProperty("delegates") List<SplittableInputSource> delegates
  )
  {
    Preconditions.checkArgument(
        delegates != null && !delegates.isEmpty(),
        "Must specify atleast one delegate inputSource"
    );
    this.delegates = delegates;
  }

  @JsonProperty
  public List<SplittableInputSource> getDelegates()
  {
    return delegates;
  }

  @Override
  public Stream<InputSplit> createSplits(
      InputFormat inputFormat,
      @Nullable SplitHintSpec splitHintSpec
  )
  {
    return delegates.stream().flatMap(inputSource -> {
      try {
        return inputSource.createSplits(inputFormat, splitHintSpec)
                          .map(inputsplit -> new InputSplit(Pair.of(inputSource, inputsplit)));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    return delegates.stream().mapToInt(inputSource -> {
      try {
        return inputSource.estimateNumSplits(inputFormat, splitHintSpec);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }).sum();
  }

  @Override
  public InputSource withSplit(InputSplit split)
  {
    Pair<SplittableInputSource, InputSplit> inputSourceWithSplit = (Pair) split.get();
    return inputSourceWithSplit.lhs.withSplit(inputSourceWithSplit.rhs);
  }

  @Override
  public boolean needsFormat()
  {
    // This is called only when ParallelIndexIngestionSpec needs to decide if either inputformat vs parserspec is required.
    // So if at least one of the delegate inputSources needsFormat, we set this to true.
    // All other needsFormat calls will be made against the delegate inputSources.
    return delegates.stream().anyMatch(SplittableInputSource::needsFormat);
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
    CombiningInputSource that = (CombiningInputSource) o;
    return delegates.equals(that.delegates);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(delegates);
  }
}
