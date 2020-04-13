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

package org.apache.druid.segment.data;

import java.io.Closeable;

/**
 * Resource that provides random access to a packed array of short arrays of ints ({@link IndexedInts}). Backs up
 * multi-valued {@link org.apache.druid.segment.column.DictionaryEncodedColumn}s.
 */
public interface ColumnarMultiInts extends Indexed<IndexedInts>, Closeable
{
  /**
   * Returns the values at a given row index. The IndexedInts object may potentially be reused, so callers should
   * not keep references to it.
   */
  @Override
  IndexedInts get(int index);

  /**
   * Returns the values at a given row index. The IndexedInts object will not be reused. This method may be less
   * efficient than plain "get".
   */
  IndexedInts getUnshared(int index);
}
