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

package io.druid.query.aggregation;

import io.druid.segment.ColumnValueSelector;

public class NullableObjectAggregateCombiner extends ObjectAggregateCombiner
{
  private boolean isNull;

  private ObjectAggregateCombiner delegate;

  public NullableObjectAggregateCombiner(ObjectAggregateCombiner delegate)
  {
    this.delegate = delegate;
  }

  @Override
  public void reset(ColumnValueSelector selector)
  {
    isNull = true;
  }

  @Override
  public void fold(ColumnValueSelector selector)
  {
    if (isNull && !selector.isNull()) {
      isNull = true;
    }
    delegate.fold(selector);
  }

  @Override
  public Class classOfObject()
  {
    return delegate.classOfObject();
  }

  @Override
  public Object get()
  {
    return delegate.get();
  }

  @Override
  public boolean isNull()
  {
    return isNull;
  }
}
