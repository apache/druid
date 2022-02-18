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

package org.apache.druid.segment.virtual;

import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;

/**
 * Provides {@link ColumnCapabilities} for both virtual and non-virtual columns by building on top of another base
 * {@link ColumnInspector}.
 *
 * {@link VirtualColumns} are provided with the base inspector so that they may potentially infer output types to
 * construct the appropriate capabilities for virtual columns, while the base inspector directly supplies the
 * capabilities for non-virtual columns.
 */
public class VirtualizedColumnInspector implements ColumnInspector
{
  protected final VirtualColumns virtualColumns;
  protected final ColumnInspector baseInspector;

  public VirtualizedColumnInspector(
      ColumnInspector baseInspector,
      VirtualColumns virtualColumns
  )
  {
    this.virtualColumns = virtualColumns;
    this.baseInspector = baseInspector;
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    return virtualColumns.getColumnCapabilitiesWithFallback(baseInspector, columnName);
  }
}
