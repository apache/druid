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

package org.apache.druid.query.aggregation.any;

import org.apache.druid.segment.BaseFloatColumnValueSelector;

import javax.annotation.Nullable;

public class FloatAnyAggregator extends NumericAnyAggregator<BaseFloatColumnValueSelector>
{
  private float foundValue;

  public FloatAnyAggregator(BaseFloatColumnValueSelector valueSelector)
  {
    super(valueSelector);
    this.foundValue = 0;
  }

  @Override
  void setFoundValue()
  {
    foundValue = valueSelector.getFloat();
  }

  @Override
  @Nullable
  public Object get()
  {
    return isNull ? null : foundValue;
  }

  @Override
  public float getFloat()
  {
    return foundValue;
  }

  @Override
  public long getLong()
  {
    return (long) foundValue;
  }

  @Override
  public double getDouble()
  {
    return (double) foundValue;
  }
}
