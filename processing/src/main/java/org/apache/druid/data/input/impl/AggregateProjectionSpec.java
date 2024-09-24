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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Lists;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonTypeName(AggregateProjectionSpec.TYPE_NAME)
public class AggregateProjectionSpec
{
  public static final String TYPE_NAME = "aggregate";

  private final String name;
  private final List<DimensionSchema> groupingColumns;
  private final VirtualColumns virtualColumns;
  private final AggregatorFactory[] aggregators;
  private final List<OrderBy> ordering;

  @JsonCreator
  public AggregateProjectionSpec(
      @JsonProperty("name") String name,
      @JsonProperty("groupingColumns") @Nullable List<DimensionSchema> groupingColumns,
      @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns,
      @JsonProperty("aggregators") @Nullable AggregatorFactory[] aggregators
  )
  {
    this.name = name;
    InvalidInput.conditionalException(
        !CollectionUtils.isNullOrEmpty(groupingColumns),
        "groupingColumns must not be null or empty"
    );
    this.groupingColumns = groupingColumns;
    this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
    this.aggregators = aggregators == null ? new AggregatorFactory[0] : aggregators;
    this.ordering = Lists.newArrayListWithCapacity(this.groupingColumns.size());
    for (DimensionSchema groupingColumn : this.groupingColumns) {
      ordering.add(OrderBy.ascending(groupingColumn.getName()));
    }
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public List<DimensionSchema> getGroupingColumns()
  {
    return groupingColumns;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public AggregatorFactory[] getAggregators()
  {
    return aggregators;
  }

  @JsonProperty
  public List<OrderBy> getOrdering()
  {
    return ordering;
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
    AggregateProjectionSpec that = (AggregateProjectionSpec) o;
    return Objects.equals(name, that.name)
           && Objects.equals(groupingColumns, that.groupingColumns)
           && Objects.equals(virtualColumns, that.virtualColumns)
           && Objects.deepEquals(aggregators, that.aggregators)
           && Objects.equals(ordering, that.ordering);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, groupingColumns, virtualColumns, Arrays.hashCode(aggregators), ordering);
  }

  @Override
  public String toString()
  {
    return "AggregateProjectionSpec{" +
           "name='" + name + '\'' +
           ", groupingColumns=" + groupingColumns +
           ", virtualColumns=" + virtualColumns +
           ", aggregators=" + Arrays.toString(aggregators) +
           ", ordering=" + ordering +
           '}';
  }
}
