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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesSketchIterator;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchAggregatorFactory;
import org.apache.druid.query.cache.CacheKeyBuilder;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

/**
 * Returns a quanitles {@link DoublesSketch} constructed from a given column of double values from a given
 * {@link ArrayOfDoublesSketch} using parameter k that determines the accuracy and size of the quantiles sketch.
 * The column number is optional (the default is 1).
 * The parameter k is optional (the default is defined in the sketch library).
 * The result is a quantiles sketch.
 * See <a href=https://datasketches.apache.org/docs/Quantiles/QuantilesOverview.html>Quantiles Sketch Overview</a>
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
    return DoublesSketchAggregatorFactory.COMPARATOR;
  }

  @Override
  public DoublesSketch compute(final Map<String, Object> combinedAggregators)
  {
    final ArrayOfDoublesSketch sketch = (ArrayOfDoublesSketch) getField().compute(combinedAggregators);
    final UpdateDoublesSketch qs = DoublesSketch.builder().setK(k).build();
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
    if (!(o instanceof ArrayOfDoublesSketchToQuantilesSketchPostAggregator)) {
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
    return new CacheKeyBuilder(AggregatorUtil.ARRAY_OF_DOUBLES_SKETCH_TO_QUANTILES_SKETCH_CACHE_TYPE_ID)
        .appendCacheable(getField())
        .appendInt(column)
        .appendInt(k)
        .build();
  }

}
