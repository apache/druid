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

package org.apache.druid.query.aggregation.datasketches.hll;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import com.yahoo.sketches.hll.Union;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Returns a union of a given list of sketches.
 */
public class HllSketchUnionPostAggregator implements PostAggregator
{

  private final String name;
  private final List<PostAggregator> fields;
  private final int lgK;
  private final TgtHllType tgtHllType;

  @JsonCreator
  public HllSketchUnionPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("fields") final List<PostAggregator> fields,
      @JsonProperty("lgK") @Nullable final Integer lgK,
      @JsonProperty("tgtHllType") @Nullable final String tgtHllType
  )
  {
    this.name = name;
    this.fields = fields;
    this.lgK = lgK == null ? HllSketchAggregatorFactory.DEFAULT_LG_K : lgK;
    this.tgtHllType = tgtHllType == null ? HllSketchAggregatorFactory.DEFAULT_TGT_HLL_TYPE
        : TgtHllType.valueOf(tgtHllType);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public List<PostAggregator> getFields()
  {
    return fields;
  }

  @JsonProperty
  public int getLgK()
  {
    return lgK;
  }

  @JsonProperty
  public String getTgtHllType()
  {
    return tgtHllType.toString();
  }

  @Override
  public Set<String> getDependentFields()
  {
    final Set<String> dependentFields = new LinkedHashSet<>();
    for (final PostAggregator field : fields) {
      dependentFields.addAll(field.getDependentFields());
    }
    return dependentFields;
  }

  @Override
  public Comparator<HllSketch> getComparator()
  {
    return HllSketchAggregatorFactory.COMPARATOR;
  }

  @Override
  public HllSketch compute(final Map<String, Object> combinedAggregators)
  {
    final Union union = new Union(lgK);
    for (final PostAggregator field : fields) {
      final HllSketch sketch = (HllSketch) field.compute(combinedAggregators);
      union.update(sketch);
    }
    return union.getResult(tgtHllType);
  }

  @Override
  public PostAggregator decorate(final Map<String, AggregatorFactory> aggregators)
  {
    return this;
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
        "name='" + name + '\'' +
        ", fields=" + fields +
        "lgK=" + lgK +
        "tgtHllType=" + tgtHllType +
        "}";
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HllSketchUnionPostAggregator)) {
      return false;
    }

    final HllSketchUnionPostAggregator that = (HllSketchUnionPostAggregator) o;

    if (!name.equals(that.name)) {
      return false;
    }
    if (!fields.equals(that.fields)) {
      return false;
    }
    if (lgK != that.getLgK()) {
      return false;
    }
    if (!tgtHllType.equals(that.tgtHllType)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fields, lgK, tgtHllType);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.HLL_SKETCH_UNION_CACHE_TYPE_ID)
        .appendString(name)
        .appendCacheablesIgnoringOrder(fields)
        .appendInt(lgK)
        .appendInt(tgtHllType.ordinal())
        .build();
  }

}
