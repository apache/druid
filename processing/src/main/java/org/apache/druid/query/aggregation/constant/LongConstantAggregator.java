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

package org.apache.druid.query.aggregation.constant;

import org.apache.druid.query.aggregation.Aggregator;

/**
 * This aggregator is a no-op aggregator with a fixed non-null output value. It can be used in scenarios where
 * result is constant such as {@link org.apache.druid.query.aggregation.GroupingAggregatorFactory}
 */
public class LongConstantAggregator implements Aggregator
{
  private final long value;

  public LongConstantAggregator(long value)
  {
    this.value = value;
  }

  @Override
  public void aggregate()
  {
    // No-op
  }

  @Override
  public Object get()
  {
    return value;
  }

  @Override
  public float getFloat()
  {
    return (float) value;
  }

  @Override
  public long getLong()
  {
    return value;
  }

  @Override
  public void close()
  {

  }
}
