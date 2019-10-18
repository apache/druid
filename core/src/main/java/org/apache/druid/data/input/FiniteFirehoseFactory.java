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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.druid.data.input.impl.InputRowParser;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.stream.Stream;

/**
 * {@link FiniteFirehoseFactory} designed for batch processing. Its implementations assume that the amount of inputs is
 * limited.
 *
 * @param <T> parser type
 * @param <S> input split type
 */
public interface FiniteFirehoseFactory<T extends InputRowParser, S> extends FirehoseFactory<T>
{
  /**
   * Returns true if this {@link FiniteFirehoseFactory} supports parallel batch indexing.
   */
  @JsonIgnore
  @Override
  default boolean isSplittable()
  {
    return true;
  }

  /**
   * Returns a {@link Stream} for {@link InputSplit}s. In parallel batch indexing, each {@link InputSplit} is processed
   * by a sub task.
   *
   * Listing splits may cause high overhead in some implementations. In this case, {@link InputSplit}s should be listed
   * lazily so that the listing overhead could be amortized.
   */
  @JsonIgnore
  Stream<InputSplit<S>> getSplits(@Nullable SplitHintSpec splitHintSpec) throws IOException;

  /**
   * Returns number of splits returned by {@link #getSplits}.
   */
  @JsonIgnore
  int getNumSplits(@Nullable SplitHintSpec splitHintSpec) throws IOException;

  /**
   * Returns the same {@link FiniteFirehoseFactory} but with the given {@link InputSplit}. The returned
   * {@link FiniteFirehoseFactory} is used by sub tasks in parallel batch indexing.
   */
  FiniteFirehoseFactory<T, S> withSplit(InputSplit<S> split);
}
