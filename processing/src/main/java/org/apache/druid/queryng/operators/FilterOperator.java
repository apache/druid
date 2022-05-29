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

import java.util.function.Predicate;

/**
 * Super-simple filter operator with the filter provided by a
 * predicate.
 */
public class FilterOperator<T> extends MappingOperator<T, T>
{
  private final Predicate<T> predicate;

  public FilterOperator(FragmentContext context, Operator<T> input, Predicate<T> pred)
  {
    super(context, input);
    this.predicate = pred;
  }

  @Override
  public T next() throws ResultIterator.EofException
  {
    while (true) {
      T nextValue = inputIter.next();
      if (predicate.test(nextValue)) {
        return nextValue;
      }
    }
  }
}
