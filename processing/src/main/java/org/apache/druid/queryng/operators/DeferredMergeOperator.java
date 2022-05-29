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

package org.apache.druid.queryng.operators;

import com.google.common.collect.Ordering;
import org.apache.druid.queryng.fragment.FragmentContext;

/**
 * Ordered merge of "operators to be named later" by client that
 * will also read the first row for each operator.
 *
 * @see {@link org.apache.druid.query.RetryQueryRunner}
 */
public class DeferredMergeOperator<T> extends AbstractMergeOperator<T>
{
  /**
   * Supplier of a collection (iterable) of inputs as defined by an
   * operator, iterator and current item. To unpack this a bit, the input
   * to the merge is the mechanism which distributes fragments across
   * multiple threads, waits for the response, checks for missing fragments
   * and retries. All this is opaque to this class which just wants the
   * final collection that should be merged. That collection is wrapped
   * in a supplier so that it is not started until {@link #open()} time.
   */
  private final Iterable<OperatorInput<T>> inputs;

  public DeferredMergeOperator(
      FragmentContext context,
      Ordering<? super T> ordering,
      int approxInputCount,
      Iterable<OperatorInput<T>> inputs
  )
  {
    super(
        context,
        ordering,
        approxInputCount == 0 ? 1 : approxInputCount
    );
    this.inputs = inputs;
  }

  @Override
  public ResultIterator<T> open()
  {
    for (OperatorInput<T> input : inputs) {
      context.registerChild(this, input.child);
      if (input.childIter != null) {
        merger.add(input);
      }
    }
    return merger;
  }
}
