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

/**
 * Limits the results from the input operator to the given number
 * of rows.
 */
public class LimitOperator<T> extends MappingOperator<T, T>
{
  protected final long limit;
  protected long rowCount;

  public LimitOperator(FragmentContext context, Operator<T> input, long limit)
  {
    super(context, input);
    this.limit = limit;
    context.register(this);
  }

  @Override
  public T next() throws EofException
  {
    if (rowCount >= limit) {
      throw Operators.eof();
    }
    T item = inputIter.next();
    rowCount++;
    return item;
  }
}
