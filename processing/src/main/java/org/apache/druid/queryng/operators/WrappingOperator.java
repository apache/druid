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

import java.util.Iterator;

/**
 * Operator which "wraps" another operator where the only behavior of
 * interest is at the start or end of the run. The iterator from the
 * input is the iterator for the output.
 */
public abstract class WrappingOperator<T> implements Operator<T>
{
  protected final FragmentContext context;
  private final Operator<T> input;
  protected State state = State.START;

  public WrappingOperator(FragmentContext context, Operator<T> input)
  {
    this.context = context;
    this.input = input;
    context.register(this);
  }

  @Override
  public Iterator<T> open()
  {
    Preconditions.checkState(state == State.START);
    Iterator<T> inputIter = input.open();
    state = State.RUN;
    onOpen();
    return inputIter;
  }

  @Override
  public void close(boolean cascade)
  {
    if (state != State.RUN) {
      return;
    }
    try {
      if (cascade) {
        input.close(cascade);
      }
    }
    finally {
      onClose();
      state = State.CLOSED;
    }
  }

  protected void onOpen()
  {
  }

  protected void onClose()
  {
  }
}
