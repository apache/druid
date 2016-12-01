/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
/**
 * Virtual columns are "views" created over a ColumnSelectorFactory. They can potentially draw from multiple
 * underlying columns, although they always present themselves as if they were a single column.
 */
public interface VirtualColumn
{
  /**
   * Output name of this column.
   *
   * @return name
   */
  String getOutputName();

  /**
   * Build a selector corresponding to this virtual column. Also provides the name that the
   * virtual column was referenced with, which is useful if this column uses dot notation.
   *
   * @param columnName the name this virtual column was referenced with
   * @param factory column selector factory
   * @return the selector
   */
  ObjectColumnSelector init(String columnName, ColumnSelectorFactory factory);

  /**
   * Indicates that this virtual column can be referenced with dot notation. For example,
   * a virtual column named "foo" could be referred to as "foo.bar" with the Cursor it is
   * registered with. In that case, init will be called with columnName "foo.bar" rather
   * than "foo".
   *
   * @return whether to use dot notation
   */
  boolean usesDotNotation();

  /**
   * Returns cache key
   *
   * @return cache key
   */
  byte[] getCacheKey();
}
