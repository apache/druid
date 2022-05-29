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

import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator.IterableOperator;

import java.util.Iterator;
import java.util.function.Supplier;

public class IterableReader<T> implements IterableOperator<T>
{
  private final FragmentContext context;
  private final Supplier<? extends Iterable<T>> source;
  private Iterator<T> inputIter;
  private int rowCount;

  public IterableReader(FragmentContext context, Supplier<? extends Iterable<T>> source)
  {
    this.context = context;
    this.source = source;
    context.register(this);
  }

  @Override
  public ResultIterator<T> open()
  {
    inputIter = source.get().iterator();
    return this;
  }

  @Override
  public T next() throws ResultIterator.EofException
  {
    if (!inputIter.hasNext()) {
      throw Operators.eof();
    }
    rowCount++;
    return inputIter.next();
  }

  @Override
  public void close(boolean cascade)
  {
    OperatorProfile profile = new OperatorProfile("list-reader");
    profile.add(OperatorProfile.ROW_COUNT_METRIC, rowCount);
    context.updateProfile(this, profile);
  }
}
