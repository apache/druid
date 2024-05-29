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

package org.apache.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * Representation of schema payload, includes information like RowSignature and aggregator factories.
 */
public class SchemaPayload
{
  private final RowSignature rowSignature;
  @Nullable
  private final Map<String, AggregatorFactory> aggregatorFactories;

  @JsonCreator
  public SchemaPayload(
      @JsonProperty("rowSignature") RowSignature rowSignature,
      @JsonProperty("aggregatorFactories") @Nullable Map<String, AggregatorFactory> aggregatorFactories
  )
  {
    this.rowSignature = rowSignature;
    this.aggregatorFactories = aggregatorFactories;
  }

  public SchemaPayload(RowSignature rowSignature)
  {
    this.rowSignature = rowSignature;
    this.aggregatorFactories = null;
  }

  @JsonProperty
  public RowSignature getRowSignature()
  {
    return rowSignature;
  }

  @Nullable
  @JsonProperty
  public Map<String, AggregatorFactory> getAggregatorFactories()
  {
    return aggregatorFactories;
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
    SchemaPayload that = (SchemaPayload) o;
    return Objects.equals(rowSignature, that.rowSignature)
           && Objects.equals(aggregatorFactories, that.aggregatorFactories);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(rowSignature, aggregatorFactories);
  }

  @Override
  public String toString()
  {
    return "SchemaPayload{" +
           "rowSignature=" + rowSignature +
           ", aggregatorFactories=" + aggregatorFactories +
           '}';
  }
}
