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
import org.apache.datasketches.Util;
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Returns a result of a specified set operation on the given array of sketches. Supported operations are:
 * union, intersection and set difference (UNION, INTERSECT, NOT).
 */
public class ArrayOfDoublesSketchSetOpPostAggregator extends ArrayOfDoublesSketchMultiPostAggregator
{

  private final ArrayOfDoublesSketchOperations.Operation operation;
  private final int nominalEntries;
  private final int numberOfValues;

  @JsonCreator
  public ArrayOfDoublesSketchSetOpPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("operation") final String operation,
      @JsonProperty("nominalEntries") @Nullable final Integer nominalEntries,
      @JsonProperty("numberOfValues") @Nullable final Integer numberOfValues,
      @JsonProperty("fields") List<PostAggregator> fields
  )
  {
    super(name, fields);
    this.operation = ArrayOfDoublesSketchOperations.Operation.valueOf(operation);
    this.nominalEntries = nominalEntries == null ? Util.DEFAULT_NOMINAL_ENTRIES : nominalEntries;
    this.numberOfValues = numberOfValues == null ? 1 : numberOfValues;
    Util.checkIfPowerOf2(this.nominalEntries, "size");

    if (fields.size() <= 1) {
      throw new IAE("Illegal number of fields[%d], must be > 1", fields.size());
    }
  }

  @Override
  public Comparator<ArrayOfDoublesSketch> getComparator()
  {
    return ArrayOfDoublesSketchAggregatorFactory.COMPARATOR;
  }

  @Override
  public ArrayOfDoublesSketch compute(final Map<String, Object> combinedAggregators)
  {
    final ArrayOfDoublesSketch[] sketches = new ArrayOfDoublesSketch[getFields().size()];
    for (int i = 0; i < sketches.length; i++) {
      sketches[i] = (ArrayOfDoublesSketch) getFields().get(i).compute(combinedAggregators);
    }
    return operation.apply(nominalEntries, numberOfValues, sketches);
  }

  @JsonProperty
  public String getOperation()
  {
    return operation.toString();
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
        + ", operation=" + operation
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
    if (!(o instanceof ArrayOfDoublesSketchSetOpPostAggregator)) {
      return false;
    }
    final ArrayOfDoublesSketchSetOpPostAggregator that = (ArrayOfDoublesSketchSetOpPostAggregator) o;
    if (nominalEntries != that.nominalEntries) {
      return false;
    }
    if (numberOfValues != that.numberOfValues) {
      return false;
    }
    return operation.equals(that.operation);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), operation, nominalEntries, numberOfValues);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.ARRAY_OF_DOUBLES_SKETCH_SET_OP_CACHE_TYPE_ID)
        .appendCacheables(getFields())
        .appendInt(nominalEntries)
        .appendInt(numberOfValues)
        .appendString(operation.toString())
        .build();
  }

}
