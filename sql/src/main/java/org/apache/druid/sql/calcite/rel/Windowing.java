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

package org.apache.druid.sql.calcite.rel;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.RowSignature;

import java.util.List;
import java.util.Objects;

/**
 * Maps onto a {@link org.apache.druid.query.operator.WindowOperatorQuery}.
 */
public class Windowing
{
  private final List<String> partitionColumns;
  private final List<AggregatorFactory> aggregators;
  private final boolean cumulative;
  private final RowSignature signature;

  public Windowing(
      final List<String> partitionColumns,
      final List<AggregatorFactory> aggregators,
      final boolean cumulative,
      final RowSignature signature
  )
  {
    this.partitionColumns = partitionColumns;
    this.aggregators = aggregators;
    this.cumulative = cumulative;
    this.signature = signature;
  }

  public List<String> getPartitionColumns()
  {
    return partitionColumns;
  }

  public List<AggregatorFactory> getAggregators()
  {
    return aggregators;
  }

  public boolean isCumulative()
  {
    return cumulative;
  }

  public RowSignature getSignature()
  {
    return signature;
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
    Windowing windowing = (Windowing) o;
    return cumulative == windowing.cumulative
           && Objects.equals(partitionColumns, windowing.partitionColumns)
           && Objects.equals(aggregators, windowing.aggregators)
           && Objects.equals(signature, windowing.signature);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(partitionColumns, aggregators, cumulative, signature);
  }

  @Override
  public String toString()
  {
    return "Windowing{" +
           "partitionColumns=" + partitionColumns +
           ", aggregators=" + aggregators +
           ", cumulative=" + cumulative +
           ", signature=" + signature +
           '}';
  }
}
