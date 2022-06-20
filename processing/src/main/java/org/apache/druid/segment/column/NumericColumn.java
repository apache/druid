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

import org.apache.druid.query.monomorphicprocessing.CalledFromHotLoop;
import org.apache.druid.query.monomorphicprocessing.HotLoopCallee;

/**
 */
public interface NumericColumn extends BaseColumn, HotLoopCallee
{
  /**
   * Returns the row count of this column.
   *
   * Note that this method currently counts only non-null values. Using this method to get the length of a column
   * that contains nulls will not work as expected. This method should be used only for non-nullable numeric columns
   * such as the "__time" column.
   */
  int length();

  @CalledFromHotLoop
  long getLongSingleValueRow(int rowNum);

  @Override
  void close();
}
