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

import org.apache.druid.common.asyncresource.SettableAsyncResource;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Handle on a best-effort warm-up started by {@link CursorFactory#prefetch(CursorBuildSpec)}, which does actions such
 * as downloading column files and deserializing column holders.
 *
 * <h3>Lifecycle</h3>
 * <ul>
 *   <li>The handle is ready ({@link #isReady()}) once the warm-up is complete, whether it succeeded or failed;
 *   {@link #get()} returns {@link Stats} on success and throws the failure otherwise.</li>
 *   <li>Keep the handle open for as long as the warmed-up parts should stay resident. Closing it after it is ready
 *   releases the holds that pin them. The cursor build that benefits from the warm-up takes its own pins on the
 *   calling thread, so close the handle only once that {@code makeCursorHolderAsync} call has returned, or the parts
 *   may be evicted in between and loaded twice.</li>
 *   <li>Closing the handle before it is ready cancels the warm-up, pending downloads are canceled and the pins
 *   released, and {@link #get()} then throws
 *   {@link org.apache.druid.common.asyncresource.AsyncResourceCanceledException}.</li>
 *   <li>Close is not idempotent; close exactly once.</li>
 * </ul>
 */
public class CursorPrefetch extends SettableAsyncResource<CursorPrefetch.Stats>
{
  /**
   * Stats about the prefetch plan.
   */
  public record Stats(List<String> bundles, int plannedRuns, long plannedBytes)
  {
    public static final Stats NONE = new Stats(List.of(), 0, 0L);
  }

  /**
   * @param canceler optional callback invoked from {@link #close()} when the handle is closed before the warm-up has
   *                 completed for producers that support cancellation.
   */
  public CursorPrefetch(@Nullable Runnable canceler)
  {
    super(false);
    if (canceler != null) {
      setCanceler(canceler);
    }
  }

  /**
   * An already-complete prefetch that did nothing and pins nothing, for sources that are always fully resident.
   */
  public static CursorPrefetch none()
  {
    final CursorPrefetch prefetch = new CursorPrefetch(null);
    prefetch.set(Stats.NONE, null);
    return prefetch;
  }
}
