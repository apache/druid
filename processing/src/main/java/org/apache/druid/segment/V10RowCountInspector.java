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

import org.apache.druid.segment.projections.ProjectionMetadata;

/**
 * {@link RowCountInspector} for V10 segments. Reads the row count from the base projection's
 * {@link ProjectionMetadata#getNumRows()} (persisted at write time), so it answers without reading any column data.
 */
public final class V10RowCountInspector implements RowCountInspector
{
  /**
   * Build an inspector that reads the row count from the given base projection metadata.
   */
  public static V10RowCountInspector forBaseProjection(ProjectionMetadata baseProjection)
  {
    return new V10RowCountInspector(baseProjection.getNumRows());
  }

  private final int numRows;

  private V10RowCountInspector(int numRows)
  {
    this.numRows = numRows;
  }

  @Override
  public int getNumRows()
  {
    return numRows;
  }
}
