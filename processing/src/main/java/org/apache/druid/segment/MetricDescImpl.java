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

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.serde.ComplexMetrics;

public class MetricDescImpl implements MetricDesc
{
  private final int index;
  private final String name;
  private final AggregatorFactory aggregatorFactory;
  private final String type;
  private final ColumnCapabilitiesImpl capabilities;

  public MetricDescImpl(int index, AggregatorFactory factory)
  {
    this.index = index;
    this.name = factory.getName();
    this.aggregatorFactory = factory;

    final String typeInfo = factory.getTypeName();
    this.capabilities = new ColumnCapabilitiesImpl();
    if ("float".equalsIgnoreCase(typeInfo)) {
      capabilities.setType(ValueType.FLOAT);
      this.type = typeInfo;
    } else if ("long".equalsIgnoreCase(typeInfo)) {
      capabilities.setType(ValueType.LONG);
      this.type = typeInfo;
    } else if ("double".equalsIgnoreCase(typeInfo)) {
      capabilities.setType(ValueType.DOUBLE);
      this.type = typeInfo;
    } else {
      capabilities.setType(ValueType.COMPLEX);
      this.type = ComplexMetrics.getSerdeForType(typeInfo).getTypeName();
    }
  }

  @Override
  public int getIndex()
  {
    return index;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public AggregatorFactory getAggregatorFactory()
  {
    return aggregatorFactory;
  }

  @Override
  public String getType()
  {
    return type;
  }

  @Override
  public ValueType getValueType()
  {
    return capabilities.getType();
  }

  @Override
  public ColumnCapabilitiesImpl getCapabilities()
  {
    return capabilities;
  }
}
