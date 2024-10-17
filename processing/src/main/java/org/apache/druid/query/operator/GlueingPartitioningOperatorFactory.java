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

package org.apache.druid.query.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

public class GlueingPartitioningOperatorFactory extends AbstractPartitioningOperatorFactory
{
  private final Integer maxRowsMaterialized;

  @JsonCreator
  public GlueingPartitioningOperatorFactory(
      @JsonProperty("partitionColumns") List<String> partitionColumns,
      @JsonProperty("maxRowsMaterialized") Integer maxRowsMaterialized
  )
  {
    super(partitionColumns);
    this.maxRowsMaterialized = maxRowsMaterialized;
  }

  @JsonProperty("maxRowsMaterialized")
  public Integer getMaxRowsMaterialized()
  {
    return maxRowsMaterialized;
  }

  @Override
  public Operator wrap(Operator op)
  {
    return new GlueingPartitioningOperator(op, partitionColumns, maxRowsMaterialized);
  }

  @Override
  public boolean validateEquivalent(OperatorFactory other)
  {
    if (!super.validateEquivalent(other)) {
      return false;
    }

    if (!(other instanceof GlueingPartitioningOperatorFactory)) {
      return false;
    }

    return Objects.equals(maxRowsMaterialized, ((GlueingPartitioningOperatorFactory) other).getMaxRowsMaterialized());
  }

  @Override
  public String toString()
  {
    return "GlueingPartitioningOperatorFactory{" +
           "partitionColumns=" + partitionColumns +
           "maxRowsMaterialized=" + maxRowsMaterialized +
           '}';
  }

  @Override
  public final int hashCode()
  {
    return Objects.hash(partitionColumns, maxRowsMaterialized);
  }

  @Override
  public final boolean equals(Object obj)
  {
    return super.equals(obj) &&
           Objects.equals(maxRowsMaterialized, ((GlueingPartitioningOperatorFactory) obj).getMaxRowsMaterialized());
  }
}
