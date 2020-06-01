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

package org.apache.druid.indexing.common.task.batch.parallel.distribution;

import org.apache.datasketches.quantiles.ItemsUnion;

/**
 * Merges {@link StringSketch}es.
 */
public class StringSketchMerger implements StringDistributionMerger
{
  private final ItemsUnion<String> delegate;

  public StringSketchMerger()
  {
    delegate = ItemsUnion.getInstance(StringSketch.SKETCH_K, StringSketch.SKETCH_COMPARATOR);
  }

  @Override
  public void merge(StringDistribution stringDistribution)
  {
    if (!(stringDistribution instanceof StringSketch)) {
      throw new IllegalArgumentException("Only merging StringSketch instances is currently supported");
    }

    StringSketch stringSketch = (StringSketch) stringDistribution;
    delegate.update(stringSketch.getDelegate());
  }

  @Override
  public StringDistribution getResult()
  {
    return new StringSketch(delegate.getResult());
  }
}
