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
import org.apache.druid.utils.CloseableUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Resource that collects a list of underlying resources into a single lifecycle.
 *
 * @see AsyncResources#collect(List) for more details
 */
public class CollectAsyncResource<T> implements AsyncResource<List<T>>
{
  private final List<AsyncResource<T>> sourceResources;
  private final AtomicInteger readyCount = new AtomicInteger(0);
  private final SettableAsyncResource<List<T>> targetResource = new SettableAsyncResource<>();

  /**
   * Constructor. Can also be created with {@link AsyncResources#collect(List)}.
   */
  CollectAsyncResource(final List<AsyncResource<T>> sourceResources)
  {
    this.sourceResources = sourceResources;

    if (sourceResources.isEmpty()) {
      targetResource.set(List.of(), null);
    } else {
      for (final AsyncResource<T> asyncResource : sourceResources) {
        asyncResource.addReadyCallback(this::onOneSourceReady);
      }
    }
  }

  @Override
  public boolean isReady()
  {
    return targetResource.isReady();
  }

  @Override
  public void addReadyCallback(Runnable callback)
  {
    targetResource.addReadyCallback(callback);
  }

  @Override
  public List<T> get()
  {
    return targetResource.get();
  }

  @Override
  public void close()
  {
    final Closer closer = Closer.create();
    closer.registerAll(sourceResources);
    closer.register(targetResource);
    CloseableUtils.closeAndWrapExceptions(closer);
  }

  private void onOneSourceReady()
  {
    if (readyCount.incrementAndGet() == sourceResources.size()) {
      try {
        final List<T> resources = new ArrayList<>(sourceResources.size());
        for (final AsyncResource<T> asyncResource : sourceResources) {
          resources.add(asyncResource.get());
        }
        targetResource.set(resources, null); // no Closeable here, since we own asyncResources ourselves
      }
      catch (Throwable e) {
        targetResource.setException(e);
      }
    }
  }
}
