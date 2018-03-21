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

package io.druid.query.aggregation.datasketches.tuple;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.PostAggregator;

import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketchIterator;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;

/**
 * Returns a quanitles DoublesSketch constructed from a given column of double values from a given
 * ArrayOfDoublesSketch using parameter k that determines the accuracy and size of the quantiles sketch.
 * The column number is optional (the default is 1).
 * The parameter k is optional (the default is defined in the sketch library).
 * The result is a quantiles sketch.
 * See https://datasketches.github.io/docs/Quantiles/QuantilesOverview.html
 */
public class ArrayOfDoublesSketchToQuantilesSketchPostAggregator extends ArrayOfDoublesSketchUnaryPostAggregator
{

  private static final int DEFAULT_QUANTILES_SKETCH_SIZE = 128;

  private final int column;
  private final int k;

  @JsonCreator
  public ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field,
      @JsonProperty("column") @Nullable final Integer column,
      @JsonProperty("k") @Nullable final Integer k
  )
  {
    super(name, field);
    this.column = column == null ? 1 : column;
    this.k = k == null ? DEFAULT_QUANTILES_SKETCH_SIZE : k;
  }

  @Override
  public Comparator<DoublesSketch> getComparator()
  {
    return new Comparator<DoublesSketch>()
    {
      @Override
      public int compare(final DoublesSketch a, final DoublesSketch b)
      {
        return Long.compare(a.getN(), b.getN());
      }
    };
  }

  @Override
  public Object compute(final Map<String, Object> combinedAggregators)
  {
    final ArrayOfDoublesSketch sketch = (ArrayOfDoublesSketch) getField().compute(combinedAggregators);
    final UpdateDoublesSketch qs = UpdateDoublesSketch.builder().setK(k).build();
    final ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      qs.update(it.getValues()[column - 1]); // convert 1-based column number to zero-based index
    }
    return qs;
  }

  @JsonProperty
  public int getColumn()
  {
    return column;
  }

  @JsonProperty
  public int getK()
  {
    return k;
  }

  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "{" +
        "name='" + getName() + '\'' +
        ", field=" + getField() +
        ", column=" + column +
        ", k=" + k +
        "}";
  }

  @Override
  public boolean equals(final Object o)
  {
    if (!super.equals(o)) {
      return false;
    }
    final ArrayOfDoublesSketchToQuantilesSketchPostAggregator that = (ArrayOfDoublesSketchToQuantilesSketchPostAggregator) o;
    if (column != that.column) {
      return false;
    }
    if (k != that.k) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), column, k);
  }

  @Override
  public byte[] getCacheKey()
  {
    return getCacheKeyBuilder().appendInt(column).appendInt(k).build();
  }

  @Override
  byte getCacheId()
  {
    return AggregatorUtil.ARRAY_OF_DOUBLES_SKETCH_TO_QUANTILES_SKETCH_CACHE_TYPE_ID;
  }

}
