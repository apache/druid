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

package org.apache.druid.segment.index;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.segment.column.ColumnIndexCapabilities;

/**
 * Common interface for bitmap indexes for use by {@link org.apache.druid.query.filter.Filter} for cursor creation, to
 * allow fast row skipping during query processing.
 */
public interface BitmapColumnIndex
{
  ColumnIndexCapabilities getIndexCapabilities();

  /**
   * Compute a bitmap result wrapped with the {@link BitmapResultFactory} representing the rows matched by this index.
   *
   * @param bitmapResultFactory helper to format the {@link org.apache.druid.collections.bitmap.ImmutableBitmap} in a
   *                            form ready for consumption by callers
   * @param includeUnknown      mapping for Druid native two state logic system into SQL three-state logic system. If set
   *                            to true, bitmaps returned by this method should include true bits for any rows where
   *                            the matching result is 'unknown', such as from the input being null valued.
   *                            See {@link NullHandling#useThreeValueLogic()}.
   *
   * @return bitmap result representing rows matched by this index
   */
  <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown);
}
