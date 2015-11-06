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

package io.druid.query.aggregation.datasketches.theta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.sketches.theta.Sketch;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.Collections;
import java.util.List;

public class SketchMergeAggregatorFactory extends SketchAggregatorFactory
{

  private static final byte CACHE_TYPE_ID = 15;

  private final boolean shouldFinalize;

  @JsonCreator
  public SketchMergeAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("size") Integer size,
      @JsonProperty("shouldFinalize") Boolean shouldFinalize
  )
  {
    super(name, fieldName, size, CACHE_TYPE_ID);
    this.shouldFinalize = (shouldFinalize == null) ? true : shouldFinalize.booleanValue();
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.<AggregatorFactory>singletonList(new SketchMergeAggregatorFactory(fieldName, fieldName, size, shouldFinalize));
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new SketchMergeAggregatorFactory(name, name, size, shouldFinalize);
  }

  @JsonProperty
  public boolean getShouldFinalize()
  {
    return shouldFinalize;
  }

  /**
   * Finalize the computation on sketch object and returns estimate from underlying
   * sketch.
   *
   * @param object the sketch object
   * @return sketch object
   */
  @Override
  public Object finalizeComputation(Object object)
  {
    if (shouldFinalize) {
      return ((Sketch) object).getEstimate();
    } else {
      return object;
    }
  }

  @Override
  public String getTypeName()
  {
    return SketchModule.THETA_SKETCH_MERGE_AGG;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    SketchMergeAggregatorFactory that = (SketchMergeAggregatorFactory) o;

    return shouldFinalize == that.shouldFinalize;

  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (shouldFinalize ? 1 : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{"
           + "fieldName='" + fieldName + '\''
           + ", name='" + name + '\''
           + ", size=" + size + '\''
           + ", shouldFinalize=" + shouldFinalize +
           + '}';
  }
}
