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

package org.apache.druid.query.aggregation;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.BaseLongColumnValueSelector;

/**
 *
 */
public class SingleValueFloatAggregator implements Aggregator
{
  final BaseLongColumnValueSelector valueSelector;

  Long value;

  public SingleValueFloatAggregator(BaseLongColumnValueSelector valueSelector)
  {
    this.valueSelector = valueSelector;
    this.value = valueSelector.getLong();
    //this.aggregatedEarlier = false;
  }

  @Override
  public void aggregate()
  {
    throw new ISE("Single Value - cant be aggregated");
  }

  @Override
  public Object get()
  {
    return this.getLong();
  }

  @Override
  public float getFloat()
  {
    return (float) this.getLong();
  }

  @Override
  public long getLong()
  {
    return value;
  }

  @Override
  public double getDouble()
  {
    return value.doubleValue();
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  @Override
  public String toString()
  {
    return "SingleValueAggregator{" +
           "valueSelector=" + valueSelector +
           ", value=" + value +
           '}';
  }
}
