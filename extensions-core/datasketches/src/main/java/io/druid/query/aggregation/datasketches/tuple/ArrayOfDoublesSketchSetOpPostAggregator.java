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
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;

import io.druid.java.util.common.IAE;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.PostAggregator;

public class ArrayOfDoublesSketchSetOpPostAggregator extends ArrayOfDoublesSketchMultiPostAggregator
{

  private final ArrayOfDoublesSketchOperations.Func func;
  private final int nominalEntries;
  private final int numberOfValues;

  @JsonCreator
  public ArrayOfDoublesSketchSetOpPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("func") final String func,
      @JsonProperty("nominalEntries") final Integer nominalEntries,
      @JsonProperty("numberOfValues") final Integer numberOfValues,
      @JsonProperty("fields") List<PostAggregator> fields
  )
  {
    super(name, fields);
    this.func = ArrayOfDoublesSketchOperations.Func.valueOf(func);
    this.nominalEntries = nominalEntries == null ? Util.DEFAULT_NOMINAL_ENTRIES : nominalEntries;
    this.numberOfValues = numberOfValues == null ? 1 : numberOfValues;
    Util.checkIfPowerOf2(this.nominalEntries, "size");

    if (fields.size() <= 1) {
      throw new IAE("Illegal number of fields[%s], must be > 1", fields.size());
    }
  }

  @Override
  public Comparator<ArrayOfDoublesSketch> getComparator()
  {
    return ArrayOfDoublesSketchAggregatorFactory.COMPARATOR;
  }

  @Override
  public Object compute(final Map<String, Object> combinedAggregators)
  {
    final ArrayOfDoublesSketch[] sketches = new ArrayOfDoublesSketch[getFields().size()];
    for (int i = 0; i < sketches.length; i++) {
      sketches[i] = (ArrayOfDoublesSketch) getFields().get(i).compute(combinedAggregators);
    }
    return ArrayOfDoublesSketchOperations.sketchSetOperation(func, nominalEntries, numberOfValues, sketches);
  }

  @JsonProperty
  public String getFunc()
  {
    return func.toString();
  }

  @JsonProperty
  public int getNominalEntries()
  {
    return nominalEntries;
  }

  @JsonProperty
  public int getNumberOfValues()
  {
    return numberOfValues;
  }

  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "{"
        + "name='" + getName() + '\''
        + ", fields=" + getFields()
        + ", func=" + func
        + ", nominalEntries=" + nominalEntries
        + ", numberOfValues=" + numberOfValues
        + "}";
  }

  @Override
  public boolean equals(final Object o)
  {
    if (!super.equals(o)) {
      return false;
    }
    final ArrayOfDoublesSketchSetOpPostAggregator that = (ArrayOfDoublesSketchSetOpPostAggregator) o;
    if (nominalEntries != that.nominalEntries) {
      return false;
    }
    if (numberOfValues != that.numberOfValues) {
      return false;
    }
    return func.equals(that.func);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), func, nominalEntries, numberOfValues);
  }

  @Override
  public byte[] getCacheKey()
  {
    return getCacheKeyBuilder().appendInt(nominalEntries).appendInt(numberOfValues)
        .appendString(func.toString()).build();
  }

  @Override
  byte getCacheId()
  {
    return AggregatorUtil.ARRAY_OF_DOUBLES_SKETCH_SET_OP_CACHE_TYPE_ID;
  }

}
