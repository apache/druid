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

package org.apache.druid.segment.column;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;

import javax.annotation.Nullable;

/**
 */
public interface ColumnHolder
{
  String TIME_COLUMN_NAME = "__time";
  String DOUBLE_STORAGE_TYPE_PROPERTY = "druid.indexing.doubleStorage";

  static boolean storeDoubleAsFloat()
  {
    String value = System.getProperty(DOUBLE_STORAGE_TYPE_PROPERTY, "double");
    return !"double".equals(StringUtils.toLowerCase(value));
  }

  ColumnCapabilities getCapabilities();

  int getLength();
  BaseColumn getColumn();
  @Nullable
  BitmapIndex getBitmapIndex();
  @Nullable
  SpatialIndex getSpatialIndex();

  /**
   * Returns a new instance of a {@link SettableColumnValueSelector}, corresponding to the type of this column.
   */
  SettableColumnValueSelector makeNewSettableColumnValueSelector();
}
