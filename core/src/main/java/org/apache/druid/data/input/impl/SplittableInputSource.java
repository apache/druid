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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.SplitHintSpec;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.stream.Stream;

/**
 * Splittable InputSource. ParallelIndexSupervisorTask can process {@link InputSplit}s in parallel.
 */
public interface SplittableInputSource<T> extends InputSource
{
  SplitHintSpec DEFAULT_SPLIT_HINT_SPEC = new MaxSizeSplitHintSpec(null);

  @JsonIgnore
  @Override
  default boolean isSplittable()
  {
    return true;
  }

  /**
   * Creates a {@link Stream} of {@link InputSplit}s. The returned stream is supposed to be evaluated lazily to avoid
   * consuming too much memory.
   * Note that this interface also has {@link #estimateNumSplits} which is related to this method. The implementations
   * should be careful to <i>NOT</i> cache the created splits in memory.
   *
   * Implementations can consider {@link InputFormat#isSplittable()} and {@link SplitHintSpec} to create splits
   * in the same way with {@link #estimateNumSplits}.
   */
  Stream<InputSplit<T>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec) throws IOException;

  /**
   * Returns an estimated total number of splits to be created via {@link #createSplits}. The estimated number of splits
   * doesn't have to be accurate and can be different from the actual number of InputSplits returned from
   * {@link #createSplits}. This will be used to estimate the progress of a phase in parallel indexing.
   * See TaskMonitor for more details of the progress estimation.
   *
   * This method can be expensive if an implementation iterates all directories or whatever substructure
   * to find all input entities.
   *
   * Implementations can consider {@link InputFormat#isSplittable()} and {@link SplitHintSpec} to find splits
   * in the same way with {@link #createSplits}.
   */
  int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec) throws IOException;

  /**
   * Helper method for ParallelIndexSupervisorTask.
   * Most of implementations can simply create a new instance with the given split.
   */
  InputSource withSplit(InputSplit<T> split);

  default SplitHintSpec getSplitHintSpecOrDefault(@Nullable SplitHintSpec splitHintSpec)
  {
    return splitHintSpec == null ? DEFAULT_SPLIT_HINT_SPEC : splitHintSpec;
  }
}
