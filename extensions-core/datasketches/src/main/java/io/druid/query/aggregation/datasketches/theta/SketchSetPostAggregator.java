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
import com.google.common.collect.Sets;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;

import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.PostAggregator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SketchSetPostAggregator implements PostAggregator
{

  private static final Logger LOG = new Logger(SketchSetPostAggregator.class);

  private final String name;
  private final List<PostAggregator> fields;
  private final SketchOperations.Func func;
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
    this.func = SketchOperations.Func.valueOf(func);
    this.maxSketchSize = maxSize == null ? SketchAggregatorFactory.DEFAULT_MAX_SKETCH_SIZE : maxSize;
    Util.checkIfPowerOf2(this.maxSketchSize, "size");

    if (fields.size() <= 1) {
      throw new IAE("Illegal number of fields[%s], must be > 1", fields.size());
    }
  }

  @Override
  public Set<String> getDependentFields()
  {
    Set<String> dependentFields = Sets.newLinkedHashSet();
    for (PostAggregator field : fields) {
      dependentFields.addAll(field.getDependentFields());
    }
    return dependentFields;
  }

  @Override
  public Comparator<Sketch> getComparator()
  {
    return SketchAggregatorFactory.COMPARATOR;
  }

  @Override
  public Object compute(final Map<String, Object> combinedAggregators)
  {
    Sketch[] sketches = new Sketch[fields.size()];
    for (int i = 0; i < sketches.length; i++) {
      sketches[i] = toSketch(fields.get(i).compute(combinedAggregators));
    }

    return SketchOperations.sketchSetOperation(func, maxSketchSize, sketches);
  }

  public final static Sketch toSketch(Object obj)
  {
    if (obj instanceof Sketch) {
      return (Sketch) obj;
    } else if (obj instanceof Union) {
      return ((Union) obj).getResult(true, null);
    } else {
      throw new IAE("Can't convert to Sketch object [%s]", obj.getClass());
    }
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
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
    return func == that.func;

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
}
