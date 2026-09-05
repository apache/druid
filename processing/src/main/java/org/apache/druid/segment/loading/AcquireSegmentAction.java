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

package org.apache.druid.segment.loading;

import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.SettableAsyncResource;
import org.apache.druid.segment.Segment;

import javax.annotation.Nullable;

/**
 * Handle for acquiring a reference to a {@link Segment} which might need to be loaded on demand: an
 * {@link AsyncResource} delivering an {@link AcquireSegmentResult} whose pre-acquired segment reference carries
 * every hold and reference associated with the acquisition inside its own {@link Segment#close()}.
 * <p>
 * The load (if one is needed) starts when the handle is created; there is no separate initiation step.
 *
 * <h3>Consumer protocol</h3>
 * Register the handle with cleanup machinery (e.g. a {@code Closer}) immediately — this is safe at any lifecycle
 * point. Wait for {@link #isReady()} via {@link #addReadyCallback} or {@link #await}, then call {@link #release()}
 * to take ownership of the {@link AcquireSegmentResult} (or surface the producer's exception). After release, the
 * caller owns closing the result (or the segment inside it); {@link #close()} on this handle becomes a no-op.
 * <p>
 * Closing the handle without releasing cancels an in-flight load (releasing any cache holds placed for it), or
 * closes an already-delivered result. Note that ready callbacks are never fired if the handle is closed before the
 * result arrives, and that {@link #close()} is <b>not</b> idempotent (it throws if called twice), close exactly
 * once.
 */
public class AcquireSegmentAction extends SettableAsyncResource<AcquireSegmentResult>
{
  /**
   * Handle representing a segment that is known to be missing: immediately ready with
   * {@link AcquireSegmentResult#empty()}.
   */
  public static AcquireSegmentAction missingSegment()
  {
    return completed(AcquireSegmentResult.empty());
  }

  /**
   * Handle that is immediately ready with the given result, for segments that did not require an asynchronous load.
   */
  public static AcquireSegmentAction completed(AcquireSegmentResult result)
  {
    final AcquireSegmentAction action = new AcquireSegmentAction();
    action.set(result);
    return action;
  }

  /**
   * Constructor for producers that install a canceler after creating whatever the canceler must cancel (calling
   * {@link #setCanceler} on a handle that has already become ready is a safe no-op).
   */
  public AcquireSegmentAction()
  {
    super(true);
  }

  /**
   * @param canceler optional callback invoked from {@link #close()} when the handle is closed before the result has
   *                 been delivered ({@link #set} or {@link #setException}). Producers that support cancellation
   *                 should provide one; producers that don't can pass {@code null}, in which case {@link #close()}
   *                 just stops observing the result.
   */
  public AcquireSegmentAction(@Nullable Runnable canceler)
  {
    this();
    if (canceler != null) {
      setCanceler(canceler);
    }
  }

  /**
   * Convenience setter: the result acts as its own closer, so closing an un-released ready handle closes the
   * delivered result. Returns false if this handle was closed before delivery, in which case the producer retains
   * ownership of the result and must close it.
   */
  public boolean set(AcquireSegmentResult result)
  {
    return super.set(result, result);
  }

  @Override // Overridden to change access from protected to public
  public synchronized AcquireSegmentResult release()
  {
    return super.release();
  }
}
