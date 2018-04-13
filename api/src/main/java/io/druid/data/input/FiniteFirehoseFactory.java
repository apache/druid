/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.druid.data.input.impl.InputRowParser;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * {@link FirehoseFactory} designed for batch processing. Its implementations assume that the amount of inputs is
 * limited.
 *
 * @param <T> parser type
 * @param <S> input split type
 */
public interface FiniteFirehoseFactory<T extends InputRowParser, S> extends FirehoseFactory<T>
{
  /**
   * Returns true if the {@link FirehoseFactory} supports parallel batch indexing.
   */
  @JsonIgnore
  @Override
  default boolean isSplittable()
  {
    return true;
  }

  /**
   * Returns an iterator of {@link InputSplit}s.
   */
  @JsonIgnore
  Stream<InputSplit<S>> getSplits() throws IOException;

  /**
   * Returns number of splits returned by {@link #getSplits()}.
   */
  @JsonIgnore
  int getNumSplits() throws IOException;

  /**
   * Returns the same {@link FiniteFirehoseFactory} but with the given {@link InputSplit}. The returned
   * {@link FiniteFirehoseFactory} is used by sub tasks in parallel batch indexing.
   */
  FiniteFirehoseFactory<T, S> withSplit(InputSplit<S> split);
}
