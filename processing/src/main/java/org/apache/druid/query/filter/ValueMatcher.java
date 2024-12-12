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

package org.apache.druid.query.filter;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.monomorphicprocessing.CalledFromHotLoop;
import org.apache.druid.query.monomorphicprocessing.HotLoopCallee;

/**
 * An object that returns a boolean indicating if the "current" row should be selected or not. The most prominent use
 * of this interface is that it is returned by the {@link Filter} "makeMatcher" method, where it is used to identify
 * selected rows for filtered cursors and filtered aggregators.
 *
 * @see org.apache.druid.query.filter.vector.VectorValueMatcher, the vectorized version
 */
public interface ValueMatcher extends HotLoopCallee
{
  /**
   * Returns true if the current row matches the condition.
   *
   * @param includeUnknown mapping for Druid native two state logic system into SQL three-state logic system. If set
   *                       to true, this method should also return true if the matching result is 'unknown', such
   *                       as from the input being null valued. See {@link NullHandling#useThreeValueLogic()}.
   *
   * @return true if the current row matches the condition, or is unknown and {@code includeUnknown} is set to true
   */
  @CalledFromHotLoop
  boolean matches(boolean includeUnknown);
}
