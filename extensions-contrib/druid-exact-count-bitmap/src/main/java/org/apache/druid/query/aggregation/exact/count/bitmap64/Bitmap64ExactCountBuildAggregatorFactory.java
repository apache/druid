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

package org.apache.druid.query.aggregation.exact.count.bitmap64;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;

public class Bitmap64ExactCountBuildAggregatorFactory extends Bitmap64ExactCountAggregatorFactory
{
  public static final ColumnType TYPE = ColumnType.ofComplex(Bitmap64ExactCountModule.BUILD_TYPE_NAME);

  @JsonCreator
  public Bitmap64ExactCountBuildAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    super(name, fieldName);
  }

  @Override
  protected byte getCacheTypeId()
  {
    return AggregatorUtil.BITMAP64_EXACT_COUNT_BUILD_CACHE_TYPE_ID;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    validateNumericColumn(metricFactory);
    return new Bitmap64ExactCountBuildAggregator(metricFactory.makeColumnValueSelector(getFieldName()));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    validateNumericColumn(metricFactory);
    return new Bitmap64ExactCountBuildBufferAggregator(metricFactory.makeColumnValueSelector(getFieldName()));
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return TYPE;
  }

  @Override
  public ColumnType getResultType()
  {
    return ColumnType.LONG;
  }

  /**
   * Ensures that the column referenced by {@link #getFieldName()} is of a numeric type when this aggregator is used
   * in a native Druid query. We must enforce the constraint here to provide a clear and early failure if the
   * query references a non-numeric (e.g., STRING) column.
   *
   * @throws IllegalArgumentException if the column exists and is not numeric.
   */
  private void validateNumericColumn(ColumnSelectorFactory metricFactory)
  {
    final ColumnCapabilities capabilities = metricFactory.getColumnCapabilities(getFieldName());
    if (capabilities != null) {
      final ValueType valueType = capabilities.getType();
      if (!valueType.isNumeric()) {
        throw new IAE(
            "Aggregation [%s] does not support column [%s] of type [%s]. Supported types: numeric.",
            Bitmap64ExactCountModule.BUILD_TYPE_NAME, getFieldName(), valueType);
      }
    }
  }
}
