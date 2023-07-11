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

package org.apache.druid.query.aggregation.datasketches.kll;

import org.apache.datasketches.kll.KllSketch;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

abstract class KllSketchMergeAggregator<SketchType extends KllSketch> implements Aggregator
{
  protected final ColumnValueSelector selector;
  @Nullable protected SketchType union;

  public KllSketchMergeAggregator(final ColumnValueSelector selector, final SketchType sketch)
  {
    this.selector = selector;
    union = sketch;
  }

  @Override
  public synchronized Object get()
  {
    return union;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public synchronized void close()
  {
    union = null;
  }
}
