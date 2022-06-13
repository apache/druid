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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator.IterableOperator;

import java.util.Iterator;

/**
 * Operator which allows pushing a row back onto the input. The "pushed"
 * row can occur at construction time, or during execution.
 */
public class PushBackOperator<T> implements IterableOperator<T>
{
  private final Operator<T> input;
  private Iterator<T> inputIter;
  private T pushed;

  public PushBackOperator(
      FragmentContext context,
      Operator<T> input,
      Iterator<T> inputIter,
      T pushed)
  {
    this.input = input;
    this.inputIter = inputIter;
    this.pushed = pushed;
    context.register(this);
  }

  public PushBackOperator(FragmentContext context, Operator<T> input)
  {
    this(context, input, null, null);
  }

  @Override
  public Iterator<T> open()
  {
    if (inputIter == null) {
      inputIter = input.open();
    }
    return this;
  }

  @Override
  public boolean hasNext()
  {
    return pushed != null || inputIter != null && inputIter.hasNext();
  }

  @Override
  public T next()
  {
    if (pushed != null) {
      T ret = pushed;
      pushed = null;
      return ret;
    }
    return inputIter.next();
  }

  public void push(T item)
  {
    if (pushed != null) {
      throw new ISE("Cannot push more than one items onto PushBackOperator");
    }
    pushed = item;
  }

  @Override
  public void close(boolean cascade)
  {
    if (cascade) {
      input.close(cascade);
    }
    inputIter = null;
    pushed = null;
  }
}
