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
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;

import java.util.Collections;
import java.util.List;

public class SketchMergeAggregatorFactory extends SketchAggregatorFactory
{

  private static final byte CACHE_TYPE_ID = 15;

  private final boolean shouldFinalize;
  private final boolean isInputThetaSketch;
  private final Integer errorBoundsStdDev;

  @JsonCreator
  public SketchMergeAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("size") Integer size,
      @JsonProperty("shouldFinalize") Boolean shouldFinalize,
      @JsonProperty("isInputThetaSketch") Boolean isInputThetaSketch,
      @JsonProperty("errorBoundsStdDev") Integer errorBoundsStdDev
  )
  {
    super(name, fieldName, size, CACHE_TYPE_ID);
    this.shouldFinalize = (shouldFinalize == null) ? true : shouldFinalize.booleanValue();
    this.isInputThetaSketch = (isInputThetaSketch == null) ? false : isInputThetaSketch.booleanValue();
    this.errorBoundsStdDev = errorBoundsStdDev;
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.<AggregatorFactory>singletonList(
        new SketchMergeAggregatorFactory(
            fieldName,
            fieldName,
            size,
            shouldFinalize,
            isInputThetaSketch,
            errorBoundsStdDev
        )
    );
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new SketchMergeAggregatorFactory(name, name, size, shouldFinalize, false, errorBoundsStdDev);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && other instanceof SketchMergeAggregatorFactory) {
      SketchMergeAggregatorFactory castedOther = (SketchMergeAggregatorFactory) other;

      return new SketchMergeAggregatorFactory(
          name,
          name,
          Math.max(size, castedOther.size),
          shouldFinalize,
          false,
          errorBoundsStdDev
      );
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @JsonProperty
  public boolean getShouldFinalize()
  {
    return shouldFinalize;
  }

  @JsonProperty
  public boolean getIsInputThetaSketch()
  {
    return isInputThetaSketch;
  }

  @JsonProperty
  public Integer getErrorBoundsStdDev()
  {
    return errorBoundsStdDev;
  }

  /**
   * Finalize the computation on sketch object and returns estimate from underlying
   * sketch.
   *
   * @param object the sketch object
   *
   * @return sketch object
   */
  @Override
  public Object finalizeComputation(Object object)
  {
    if (shouldFinalize) {
      Sketch sketch = (Sketch) object;
      if (errorBoundsStdDev != null) {
        SketchEstimateWithErrorBounds result = new SketchEstimateWithErrorBounds(
            sketch.getEstimate(),
            sketch.getUpperBound(errorBoundsStdDev),
            sketch.getLowerBound(errorBoundsStdDev),
            errorBoundsStdDev);
        return result;
      } else {
        return sketch.getEstimate();
      }
    } else {
      return object;
    }
  }

  @Override
  public String getTypeName()
  {
    if (isInputThetaSketch) {
      return SketchModule.THETA_SKETCH_MERGE_AGG;
    } else {
      return SketchModule.THETA_SKETCH_BUILD_AGG;
    }
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

    if (shouldFinalize != that.shouldFinalize) {
      return false;
    }
    if (errorBoundsStdDev != that.errorBoundsStdDev) {
      return false;
    }
    return isInputThetaSketch == that.isInputThetaSketch;

  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (shouldFinalize ? 1 : 0);
    result = 31 * result + (isInputThetaSketch ? 1 : 0);
    result = 31 * result + (errorBoundsStdDev != null ? errorBoundsStdDev.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "SketchMergeAggregatorFactory{"
           + "fieldName=" + fieldName
           + ", name=" + name
           + ", size=" + size
           + ", shouldFinalize=" + shouldFinalize
           + ", isInputThetaSketch=" + isInputThetaSketch
           + ", errorBoundsStdDev=" + errorBoundsStdDev
           + "}";
  }
}
