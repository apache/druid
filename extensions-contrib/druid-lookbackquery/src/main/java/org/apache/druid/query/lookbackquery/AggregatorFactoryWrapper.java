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
package org.apache.druid.query.lookbackquery;

import java.util.Comparator;
import java.util.List;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;

public class AggregatorFactoryWrapper extends AggregatorFactory
{

  private final AggregatorFactory aggregator;
  private final String prefix;

  public AggregatorFactoryWrapper(AggregatorFactory af, String prefix)
  {
    this.aggregator = af;
    this.prefix = prefix;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return aggregator.factorize(metricFactory);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return aggregator.factorizeBuffered(metricFactory);
  }

  @Override
  public Comparator getComparator()
  {
    return aggregator.getComparator();
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return aggregator.combine(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return aggregator.getCombiningFactory();
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return aggregator.getRequiredColumns();
  }

  @Override
  public Object deserialize(Object object)
  {
    return aggregator.deserialize(object);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return aggregator.finalizeComputation(object);
  }

  /**
   * Returns AggregatorFactory name prefixed with prefix
   */
  @Override
  public String getName()
  {
    return prefix + aggregator.getName();
  }

  @Override
  public List<String> requiredFields()
  {
    return aggregator.requiredFields();
  }

  @Override
  public byte[] getCacheKey()
  {
    return aggregator.getCacheKey();
  }

  @Override
  public String getTypeName()
  {
    return aggregator.getTypeName();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return aggregator.getMaxIntermediateSize();
  }
}
