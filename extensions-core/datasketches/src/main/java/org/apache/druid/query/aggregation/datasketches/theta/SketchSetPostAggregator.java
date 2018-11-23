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

package org.apache.druid.query.aggregation.datasketches.theta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.sketches.Util;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.PostAggregatorIds;
import org.apache.druid.query.cache.CacheKeyBuilder;

import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SketchSetPostAggregator implements PostAggregator
{
  private final String name;
  private final List<PostAggregator> fields;
  private final SketchHolder.Func func;
  private final int maxSketchSize;

  @JsonCreator
  public SketchSetPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("func") String func,
      @JsonProperty("size") Integer maxSize,
      @JsonProperty("fields") List<PostAggregator> fields
  )
  {
    this.name = name;
    this.fields = fields;
    this.func = SketchHolder.Func.valueOf(func);
    this.maxSketchSize = maxSize == null ? SketchAggregatorFactory.DEFAULT_MAX_SKETCH_SIZE : maxSize;
    Util.checkIfPowerOf2(this.maxSketchSize, "size");

    if (fields.size() <= 1) {
      throw new IAE("Illegal number of fields[%s], must be > 1", fields.size());
    }
  }

  @Override
  public Set<String> getDependentFields()
  {
    Set<String> dependentFields = new LinkedHashSet<>();
    for (PostAggregator field : fields) {
      dependentFields.addAll(field.getDependentFields());
    }
    return dependentFields;
  }

  @Override
  public Comparator<Object> getComparator()
  {
    return SketchHolder.COMPARATOR;
  }

  @Override
  public Object compute(final Map<String, Object> combinedAggregators)
  {
    Object[] sketches = new Object[fields.size()];
    for (int i = 0; i < sketches.length; i++) {
      sketches[i] = fields.get(i).compute(combinedAggregators);
    }

    return SketchHolder.sketchSetOperation(func, maxSketchSize, sketches);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public PostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    return this;
  }

  @JsonProperty
  public String getFunc()
  {
    return func.toString();
  }

  @JsonProperty
  public List<PostAggregator> getFields()
  {
    return fields;
  }

  @JsonProperty
  public int getSize()
  {
    return maxSketchSize;
  }

  @Override
  public String toString()
  {
    return "SketchSetPostAggregator{"
           + "name='"
           + name
           + '\''
           + ", fields="
           + fields
           + ", func="
           + func
           + ", size="
           + maxSketchSize
           + "}";
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

    SketchSetPostAggregator that = (SketchSetPostAggregator) o;

    if (maxSketchSize != that.maxSketchSize) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }
    if (!fields.equals(that.fields)) {
      return false;
    }
    return func.equals(that.func);

  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fields.hashCode();
    result = 31 * result + func.hashCode();
    result = 31 * result + maxSketchSize;
    return result;
  }

  @Override
  public byte[] getCacheKey()
  {
    final CacheKeyBuilder builder = new CacheKeyBuilder(PostAggregatorIds.DATA_SKETCHES_SKETCH_SET)
        .appendString(getFunc())
        .appendInt(maxSketchSize);

    if (preserveFieldOrderInCacheKey(func)) {
      builder.appendCacheables(fields);
    } else {
      builder.appendCacheablesIgnoringOrder(fields);
    }

    return builder.build();
  }

  private static boolean preserveFieldOrderInCacheKey(SketchHolder.Func func)
  {
    switch (func) {
      case NOT:
        return true;
      case UNION:
      case INTERSECT:
        return false;
      default:
        throw new IAE(func.name());
    }
  }
}
