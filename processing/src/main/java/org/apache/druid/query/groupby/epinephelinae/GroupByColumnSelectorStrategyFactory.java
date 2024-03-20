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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.dimension.ColumnSelectorStrategyFactory;
import org.apache.druid.query.groupby.epinephelinae.column.DictionaryBuildingGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.FixedWidthGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.PrebuiltDictionaryStringGroupByColumnSelectorStrategy;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;

/**
 * Creates {@link org.apache.druid.query.dimension.ColumnSelectorStrategy}s for grouping dimensions
 */
public class GroupByColumnSelectorStrategyFactory implements ColumnSelectorStrategyFactory<GroupByColumnSelectorStrategy>
{
  @Override
  public GroupByColumnSelectorStrategy makeColumnSelectorStrategy(
      ColumnCapabilities capabilities,
      ColumnValueSelector selector
  )
  {
    if (capabilities == null || capabilities.getType() == null) {
      throw DruidException.defensive("Unable to deduce type for the grouping dimension");
    }
    switch (capabilities.getType()) {
      case STRING:
        DimensionSelector dimSelector = (DimensionSelector) selector;
        if (dimSelector.getValueCardinality() >= 0 && dimSelector.nameLookupPossibleInAdvance()) {
          return PrebuiltDictionaryStringGroupByColumnSelectorStrategy.forType(
              ColumnType.STRING,
              selector,
              capabilities
          );
        } else {
          return DictionaryBuildingGroupByColumnSelectorStrategy.forType(ColumnType.STRING);
        }
      case LONG:
        return new FixedWidthGroupByColumnSelectorStrategy<Long>(
            Byte.BYTES + Long.BYTES,
            true,
            ColumnType.LONG
        );
      case FLOAT:
        return new FixedWidthGroupByColumnSelectorStrategy<Float>(
            Byte.BYTES + Float.BYTES,
            true,
            ColumnType.FLOAT
        );
      case DOUBLE:
        return new FixedWidthGroupByColumnSelectorStrategy<Double>(
            Byte.BYTES + Double.BYTES,
            true,
            ColumnType.DOUBLE
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
}
