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

package org.apache.druid.common.asyncresource;

import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.CloseableUtils;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Resource that passes through an underlying resource, but can substitute a fallback on failure.
 *
 * @see AsyncResources#recover(AsyncResource, Function) for more details
 */
public class RecoverAsyncResource<T> implements AsyncResource<T>
{
  private static final Logger log = new Logger(RecoverAsyncResource.class);

  private final AsyncResource<T> sourceResource;
  private final Function<Throwable, T> recovery;
  private final SettableAsyncResource<T> targetResource = new SettableAsyncResource<>();

  /**
   * Whether {@link #sourceResource} has been closed. The source is closed either after recovery (so its resources
   * are released before handing the recovered object to the caller), or otherwise in {@link #close()}.
   */
  private final AtomicBoolean sourceClosed = new AtomicBoolean(false);

  /**
   * Constructor. Can also be created with {@link AsyncResources#recover(AsyncResource, Function)}.
   */
  RecoverAsyncResource(
      final AsyncResource<T> sourceResource,
      final Function<Throwable, T> recovery
  )
  {
    this.sourceResource = sourceResource;
    this.recovery = recovery;
    sourceResource.addReadyCallback(this::onSourceReady);
  }

  @Override
  public boolean isReady()
  {
    return targetResource.isReady();
  }

  @Override
  public T get()
  {
    return targetResource.get();
  }

  @Override
  public void addReadyCallback(Runnable callback)
  {
    targetResource.addReadyCallback(callback);
  }

  @Override
  public void close()
  {
    final Closer closer = Closer.create();
    if (sourceClosed.compareAndSet(false, true)) {
      closer.register(sourceResource);
    }
    closer.register(targetResource);
    CloseableUtils.closeAndWrapExceptions(closer);
  }

  private void onSourceReady()
  {
    final T value;
    try {
      value = sourceResource.get();
    }
    catch (Throwable e) {
      final T recovered;
      try {
        recovered = recovery.apply(e);
      }
      catch (Throwable e2) {
        e.addSuppressed(e2);
        targetResource.setException(e);
        return;
      }

      if (recovered != null) {
        // Release the source's resources before handing back the recovered object.
        if (sourceClosed.compareAndSet(false, true)) {
          CloseableUtils.closeAndSuppressExceptions(
              sourceResource,
              ex -> log.warn(ex, "Failed to close source resource during recovery")
          );
        }
        targetResource.set(recovered, null);
      } else {
        targetResource.setException(e);
      }
      return;
    }

    targetResource.set(value, null);
  }
}
