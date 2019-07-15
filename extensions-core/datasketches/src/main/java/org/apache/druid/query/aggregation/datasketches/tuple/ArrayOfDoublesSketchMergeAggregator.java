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

package org.apache.druid.query.aggregation.datasketches.tuple;

import com.yahoo.sketches.tuple.ArrayOfDoublesSetOperationBuilder;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUnion;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import javax.annotation.Nullable;

/**
 * This aggregator merges existing sketches.
 * The input column contains ArrayOfDoublesSketch.
 * The output is {@link ArrayOfDoublesSketch} that is a union of the input sketches.
 */
public class ArrayOfDoublesSketchMergeAggregator implements Aggregator
{

  private final BaseObjectColumnValueSelector<ArrayOfDoublesSketch> selector;
  @Nullable
  private ArrayOfDoublesUnion union;

  public ArrayOfDoublesSketchMergeAggregator(
      final BaseObjectColumnValueSelector<ArrayOfDoublesSketch> selector,
      final int nominalEntries,
      final int numberOfValues
  )
  {
    this.selector = selector;
    union = new ArrayOfDoublesSetOperationBuilder().setNominalEntries(nominalEntries).setNumberOfValues(numberOfValues)
        .buildUnion();
  }

  /**
   * This method uses synchronization because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently
   * https://github.com/apache/incubator-druid/pull/3956
   */
  @Override
  public void aggregate()
  {
    final ArrayOfDoublesSketch update = selector.getObject();
    if (update == null) {
      return;
    }
    synchronized (this) {
      union.update(update);
    }
  }

  /**
   * This method uses synchronization because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently
   * https://github.com/apache/incubator-druid/pull/3956
   * The returned sketch is a separate instance of ArrayOfDoublesCompactSketch
   * representing the current state of the aggregation, and is not affected by consequent
   * aggregate() calls
   */
  @Override
  public synchronized Object get()
  {
    return union.getResult();
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
  public void close()
  {
    union = null;
  }

}
