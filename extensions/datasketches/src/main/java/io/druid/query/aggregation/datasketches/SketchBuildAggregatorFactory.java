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

package io.druid.query.aggregation.datasketches;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.sketches.theta.Sketch;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.Arrays;
import java.util.List;

/**
 */
public class SketchBuildAggregatorFactory extends SketchAggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 17;

  @JsonCreator
  public SketchBuildAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("size") Integer size
  )
  {
    super(name, fieldName, size, CACHE_TYPE_ID);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new SketchBuildAggregatorFactory(name, name, size);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((Sketch) object).getEstimate();
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new SketchBuildAggregatorFactory(fieldName, fieldName, size));
  }

  @Override
  public String getTypeName()
  {
    return SketchModule.SKETCH_BUILD;
  }
}
