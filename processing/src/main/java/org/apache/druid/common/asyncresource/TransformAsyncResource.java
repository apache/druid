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

import java.util.function.Function;

/**
 * Resource that is the result of calling a function on an underlying resource.
 *
 * @see AsyncResources#transform(AsyncResource, Function) for more details
 */
public class TransformAsyncResource<T, R> implements AsyncResource<R>
{
  private final AsyncResource<T> sourceResource;
  private final Function<T, R> function;
  private final SettableAsyncResource<R> targetResource;

  /**
   * Constructor. Can also be created with {@link AsyncResources#transform(AsyncResource, Function)}.
   */
  TransformAsyncResource(final AsyncResource<T> sourceResource, final Function<T, R> function)
  {
    this.sourceResource = sourceResource;
    this.function = function;
    this.targetResource = new SettableAsyncResource<>();
    sourceResource.addReadyCallback(this::onSourceReady);
  }

  @Override
  public boolean isReady()
  {
    return targetResource.isReady();
  }

  @Override
  public R get()
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
    closer.register(sourceResource);
    closer.register(targetResource);
    CloseableUtils.closeAndWrapExceptions(closer);
  }

  private void onSourceReady()
  {
    try {
      final R target = function.apply(sourceResource.get());
      targetResource.set(target, null); // no Closeable here, since we own sourceResource ourselves
    }
    catch (Throwable e) {
      targetResource.setException(e);
    }
  }
}
