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

import org.apache.druid.segment.ColumnValueSelector;

final class DoubleMinAggregateCombiner extends DoubleAggregateCombiner
{
  private double min;

  @Override
  public void reset(ColumnValueSelector selector)
  {
    min = selector.getDouble();
  }

  @Override
  public void fold(ColumnValueSelector selector)
  {
    min = Math.min(min, selector.getDouble());
  }

  @Override
  public double getDouble()
  {
    return min;
  }
}
