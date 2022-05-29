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

import com.google.common.base.Preconditions;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator.IterableOperator;

/**
 * Base class for operators that do a simple mapping of their input
 * to their output. Handles the busy-work of managing the (single)
 * input operator.
 */
public abstract class MappingOperator<IN, OUT> implements IterableOperator<OUT>
{
  protected final FragmentContext context;
  private final Operator<IN> input;
  protected ResultIterator<IN> inputIter;
  protected State state = State.START;

  public MappingOperator(FragmentContext context, Operator<IN> input)
  {
    this.context = context;
    this.input = input;
    context.register(this);
    context.registerChild(this, input);
  }

  @Override
  public ResultIterator<OUT> open()
  {
    Preconditions.checkState(state == State.START);
    inputIter = input.open();
    state = State.RUN;
    return this;
  }

  @Override
  public void close(boolean cascade)
  {
    if (state == State.RUN && cascade) {
      input.close(cascade);
    }
    inputIter = null;
    state = State.CLOSED;
  }
}
