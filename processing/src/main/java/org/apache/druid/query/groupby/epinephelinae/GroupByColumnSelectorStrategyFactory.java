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

package org.apache.druid.query.groupby.epinephelinae;

import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.dimension.ColumnSelectorStrategyFactory;
import org.apache.druid.query.groupby.epinephelinae.column.DictionaryBuildingGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.FixedWidthGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.KeyMappingMultiValueGroupByColumnSelectorStrategy;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;

/**
 * Creates {@link org.apache.druid.query.dimension.ColumnSelectorStrategy}s for grouping dimensions
 * If the type is STRING, then it delegates the group by handling to {@link KeyMappingMultiValueGroupByColumnSelectorStrategy}
 * which is specialized for {@link DimensionSelector}s and multi-value dimensions.
 * If the type is numeric, then it delegates the handling to the {@link FixedWidthGroupByColumnSelectorStrategy}
 * Else, it delegates the handling to {@link DictionaryBuildingGroupByColumnSelectorStrategy} which is a generic strategy
 * and builds dictionaries on the fly.
 */
public class GroupByColumnSelectorStrategyFactory implements ColumnSelectorStrategyFactory<GroupByColumnSelectorStrategy>
{
  @Override
  public GroupByColumnSelectorStrategy makeColumnSelectorStrategy(
      ColumnCapabilities capabilities,
      ColumnValueSelector selector,
      String dimension
  )
  {
    if (capabilities == null || capabilities.getType() == null) {
      throw DruidException.defensive("Unable to deduce type for the grouping dimension");
    }
    try {
      if (!capabilities.toColumnType().getNullableStrategy().groupable()) {
        // InvalidInput because the SQL planner would have already flagged these dimensions, therefore this will only happen
        // if native queries have been submitted.
        throw InvalidInput.exception(
            "Unable to group on the column[%s] with type[%s]",
            dimension,
            capabilities.toColumnType()
        );
      }
    }
    catch (Exception e) {
      throw InvalidInput.exception(e, "Unable to group on the column[%s]", dimension);
    }

    switch (capabilities.getType()) {
      case STRING:
        return KeyMappingMultiValueGroupByColumnSelectorStrategy.create(capabilities, (DimensionSelector) selector);
      case LONG:
        return new FixedWidthGroupByColumnSelectorStrategy<>(
            Byte.BYTES + Long.BYTES,
            ColumnType.LONG,
            ColumnValueSelector::getLong,
            ColumnValueSelector::isNull
        );
      case FLOAT:
        return new FixedWidthGroupByColumnSelectorStrategy<>(
            Byte.BYTES + Float.BYTES,
            ColumnType.FLOAT,
            ColumnValueSelector::getFloat,
            ColumnValueSelector::isNull
        );
      case DOUBLE:
        return new FixedWidthGroupByColumnSelectorStrategy<>(
            Byte.BYTES + Double.BYTES,
            ColumnType.DOUBLE,
            ColumnValueSelector::getDouble,
            ColumnValueSelector::isNull
        );
      case ARRAY:
        switch (capabilities.getElementType().getType()) {
          case LONG:
          case STRING:
          case DOUBLE:
            return DictionaryBuildingGroupByColumnSelectorStrategy.forType(capabilities.toColumnType());
          case FLOAT:
            // Array<Float> not supported in expressions, ingestion
          default:
            throw new IAE("Cannot create query type helper from invalid type [%s]", capabilities.asTypeString());

        }
      case COMPLEX:
        return DictionaryBuildingGroupByColumnSelectorStrategy.forType(capabilities.toColumnType());
      default:
        throw new IAE("Cannot create query type helper from invalid type [%s]", capabilities.asTypeString());
    }
  }

  @Override
  public boolean supportsComplexTypes()
  {
    return true;
  }
}
