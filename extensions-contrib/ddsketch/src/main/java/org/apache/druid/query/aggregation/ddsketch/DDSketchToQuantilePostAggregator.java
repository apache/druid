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

package org.apache.druid.query.aggregation.ddsketch;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.PostAggregatorIds;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnType;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Post aggregation operator that can take in aggregated ddsketches and
 * generate quantiles from it.
 */
public class DDSketchToQuantilePostAggregator implements PostAggregator
{
  private final String name;
  private final PostAggregator field;

  private final double fraction;

  public static final String TYPE_NAME = "quantileFromDDSketch";
  private static final EmittingLogger log = new EmittingLogger(DDSketchToQuantilePostAggregator.class);

  @JsonCreator
  public DDSketchToQuantilePostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field,
      @JsonProperty("fraction") final double fraction
  )
  {
    this.name = Preconditions.checkNotNull(name, "name is null");
    this.field = Preconditions.checkNotNull(field, "field is null");
    this.fraction = fraction;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public ColumnType getType(ColumnInspector signature)
  {
    return ColumnType.DOUBLE;
  }

  @JsonProperty
  public PostAggregator getField()
  {
    return field;
  }

  @JsonProperty
  public double getFraction()
  {
    return fraction;
  }

  @Override
  public Object compute(final Map<String, Object> combinedAggregators)
  {
    final DDSketch sketch = (DDSketch) field.compute(combinedAggregators);

    if (sketch == null || sketch.getCount() == 0) {
      return Double.NaN;
    }
    return sketch.getValueAtQuantile(fraction);
  }

  @Override
  public Comparator<Double> getComparator()
  {
    return Doubles::compare;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return field.getDependentFields();
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "name='" + name + '\'' +
           ", field=" + field +
           ", fraction=" + fraction +
           "}";
  }

  @Override
  public byte[] getCacheKey()
  {
    final CacheKeyBuilder builder = new CacheKeyBuilder(PostAggregatorIds.DDSKETCH_QUANTILE_TYPE_ID).appendCacheable(field);
    builder.appendDouble(fraction);
    return builder.build();
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
    DDSketchToQuantilePostAggregator that = (DDSketchToQuantilePostAggregator) o;
    return Double.compare(that.fraction, fraction) == 0 &&
           Objects.equals(name, that.name) &&
           Objects.equals(field, that.field);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, field, fraction);
  }

  @Override
  public PostAggregator decorate(final Map<String, AggregatorFactory> map)
  {
    return this;
  }

}
