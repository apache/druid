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

package org.apache.druid.query.groupby.epinephelinae.column;

import org.apache.druid.error.DruidException;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;

/**
 * Implementation of {@link KeyMappingGroupByColumnSelectorStrategy} that relies on a prebuilt dictionary to map the
 * dimension to the dictionaryId. It is more like a helper class, that handles the different ways that dictionaries can be
 * provided for different types. Array dimensions are backed by dictionaries, but not exposed via the ColumnValueSelector interface,
 * hence this strategy cannot handle array dimensions currently.
 */
public class PrebuiltDictionaryGroupByColumnSelectorStrategy
{
  /**
   * Create the strategy for the provided column type
   */
  public static GroupByColumnSelectorStrategy forType(
      final ColumnType columnType,
      final ColumnValueSelector columnValueSelector,
      final ColumnCapabilities columnCapabilities
  )
  {
    throw DruidException.defensive("Only string columns expose prebuilt dictionaries, and they should be "
                                   + "handled separately");
  }
}
