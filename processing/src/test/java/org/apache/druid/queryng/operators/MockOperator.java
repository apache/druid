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

import java.util.Iterator;
import java.util.function.Function;

public class MockOperator<T> implements IterableOperator<T>
{
  public final int targetCount;
  private final Function<Integer, T> generator;
  private int rowPosn;
  public State state = State.START;

  public MockOperator(
      FragmentContext context,
      int rowCount,
      Function<Integer, T> generator)
  {
    this.targetCount = rowCount;
    this.generator = generator;
    context.register(this);
  }

  public static MockOperator<Integer> ints(FragmentContext context, int rowCount)
  {
    return new MockOperator<Integer>(context, rowCount, rid -> rid);
  }

  public static MockOperator<String> strings(FragmentContext context, int rowCount)
  {
    return new MockOperator<String>(context, rowCount, rid -> "Mock row " + Integer.toString(rid));
  }

  @Override
  public Iterator<T> open()
  {
    Preconditions.checkState(state == State.START);
    state = State.RUN;
    return this;
  }

  @Override
  public boolean hasNext()
  {
    return rowPosn < targetCount;
  }

  @Override
  public T next()
  {
    return generator.apply(rowPosn++);
  }

  @Override
  public void close(boolean cascade)
  {
    state = State.CLOSED;
  }
}
