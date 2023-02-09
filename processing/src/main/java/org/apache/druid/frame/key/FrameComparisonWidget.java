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

package org.apache.druid.frame.key;

import org.apache.druid.frame.Frame;

/**
 * Wraps a {@link Frame} and provides ways to compare rows of that frame to various other things.
 *
 * Not thread-safe.
 */
public interface FrameComparisonWidget
{
  /**
   * Returns the {@link RowKey} corresponding to a particular row. The returned key is a copy that does
   * not reference memory of the underlying {@link Frame}.
   */
  RowKey readKey(int row);

  /**
   * Compare a specific row of this frame to the provided key. The key must have been created with sortColumns
   * that match the ones used to create this widget, or else results are undefined.
   */
  int compare(int row, RowKey key);

  /**
   * Compare a specific row of this frame to a specific row of another frame. The other frame must have the same
   * signature, or else results are undefined. The other frame may be the same object as this frame; for example,
   * this is used by {@link org.apache.druid.frame.write.FrameSort} to sort frames in-place.
   */
  int compare(int row, FrameComparisonWidget otherWidget, int otherRow);
}
