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

package org.apache.druid.segment.nested;

import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

public interface NestedColumnTypeInspector
{
  /**
   * Get list of fields represented as a sequence of {@link NestedPathPart}
   */
  List<List<NestedPathPart>> getNestedFields();

  /**
   * Get all {@link ColumnType} for the nested field column
   */
  @Nullable
  Set<ColumnType> getFieldTypes(List<NestedPathPart> path);

  /**
   * Reduces {@link #getFieldTypes(List)} for the nested field column using
   * {@link ColumnType#leastRestrictiveType(ColumnType, ColumnType)}
   */
  @Nullable
  ColumnType getFieldLogicalType(List<NestedPathPart> path);

  /**
   * Shortcut to check if a nested field column is {@link ColumnType#isNumeric()}, useful when broadly choosing the
   * type of vector selector to be used when dealing with the path
   */
  boolean isNumeric(List<NestedPathPart> path);
}
