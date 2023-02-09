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

/**
 * Returned by {@link ColumnSelectorFactory#getRowIdSupplier()}. Allows users of {@link ColumnSelectorFactory}
 * to cache objects returned by their selectors.
 */
public interface RowIdSupplier
{
  /**
   * A number that will never be returned from {@link #getRowId()}. Useful for initialization.
   */
  long INIT = -1;

  /**
   * Returns a number that uniquely identifies the current position of some underlying cursor. This is useful for
   * caching: it is safe to assume nothing has changed in the selector as long as the row ID stays the same.
   *
   * Row IDs do not need to be contiguous or monotonic. They need not have any meaning. In particular: they may not
   * be row *numbers* (row number 0 may have any arbitrary row ID).
   *
   * Valid row IDs are always nonnegative.
   */
  long getRowId();
}
