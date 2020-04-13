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

package org.apache.druid.query.aggregation.momentsketch.aggregator;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.momentsketch.MomentSketchWrapper;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;

public class MomentSketchBuildAggregator implements Aggregator
{
  private final BaseDoubleColumnValueSelector valueSelector;
  private final int k;
  private final boolean compress;

  private MomentSketchWrapper momentsSketch;

  public MomentSketchBuildAggregator(
      final BaseDoubleColumnValueSelector valueSelector,
      final int k,
      final boolean compress
  )
  {
    this.valueSelector = valueSelector;
    this.k = k;
    this.compress = compress;
    momentsSketch = new MomentSketchWrapper(k);
    momentsSketch.setCompressed(compress);
  }

  @Override
  public void aggregate()
  {
    if (valueSelector.isNull()) {
      return;
    }
    momentsSketch.add(valueSelector.getDouble());
  }

  @Override
  public Object get()
  {
    return momentsSketch;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Aggregator clone()
  {
    return new MomentSketchBuildAggregator(valueSelector, k, compress);
  }

  @Override
  public void close()
  {
    momentsSketch = null;
  }
}
