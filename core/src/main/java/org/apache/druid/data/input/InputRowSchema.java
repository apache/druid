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

package org.apache.druid.data.input;

import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;

/**
 * Schema of {@link InputRow}.
 */
public class InputRowSchema
{
  private final TimestampSpec timestampSpec;
  private final DimensionsSpec dimensionsSpec;
  private final ColumnsFilter columnsFilter;

  public InputRowSchema(
      final TimestampSpec timestampSpec,
      final DimensionsSpec dimensionsSpec,
      final ColumnsFilter columnsFilter
  )
  {
    this.timestampSpec = timestampSpec;
    this.dimensionsSpec = dimensionsSpec;
    this.columnsFilter = columnsFilter;
  }

  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }

  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  /**
   * A {@link ColumnsFilter} that can filter down the list of columns that must be read after flattening.
   *
   * Logically, Druid applies ingestion spec components in a particular order: first flattenSpec (if any), then
   * timestampSpec, then transformSpec, and finally dimensionsSpec and metricsSpec.
   *
   * If a flattenSpec is provided, this method returns a filter that should be applied after flattening. So, it will
   * be based on what needs to pass between the flattenSpec and everything beyond it.
   */
  public ColumnsFilter getColumnsFilter()
  {
    return columnsFilter;
  }
}
