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

package org.apache.druid.query.movingaverage.averagers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.sketches.Util;
import org.apache.druid.query.aggregation.datasketches.theta.SketchHolder;

import java.util.Comparator;

public class SketchUnionAveragerFactory extends BaseAveragerFactory<SketchHolder, Double>
{

  private final int size;

  @JsonCreator
  public SketchUnionAveragerFactory(
      @JsonProperty("name") String name,
      @JsonProperty("buckets") int numBuckets,
      @JsonProperty("cycleSize") Integer cycleSize,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty(value = "size", required = false) Integer size
  )
  {
    super(name, numBuckets, fieldName, cycleSize);
    this.size = size == null ? Util.DEFAULT_NOMINAL_ENTRIES : size;
  }

  public SketchUnionAveragerFactory(
      String name,
      int numBuckets,
      Integer cycleSize,
      String fieldName
  )
  {
    this(name, numBuckets, cycleSize, fieldName, null);
  }

  @Override
  public Averager<SketchHolder> createAverager()
  {
    return new SketchUnionAverager(numBuckets, name, fieldName, cycleSize, size);
  }

  /* (non-Javadoc)
   * @see AveragerFactory#finalize(java.lang.Object)
   */
  @Override
  public Double finalizeComputation(SketchHolder val)
  {
    return val.getSketch().getEstimate();
  }

  /* (non-Javadoc)
   * @see BaseAveragerFactory#getComparator()
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public Comparator getComparator()
  {
    return SketchHolder.COMPARATOR;
  }
}
