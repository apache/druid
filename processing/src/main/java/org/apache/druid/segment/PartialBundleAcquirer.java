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

import org.apache.druid.common.asyncresource.AsyncResource;

import java.io.Closeable;
import java.util.concurrent.Callable;

/**
 * Context object that bundles together pieces of cache-layer machinery needed to do on-demand partial segment loading:
 * <ol>
 *   <li>A hook for acquiring a hold on the cache-layer bundle.</li>
 *   <li>A hook for submitting bundle download tasks.</li>
 * </ol>
 */
public interface PartialBundleAcquirer
{
  /**
   * Acquire a hold on the cache-layer bundle with the given name. The returned {@link Closeable} releases the hold
   * when closed (the bundle itself stays in the cache for re-use by subsequent acquires).
   * <p>
   * Implementations must be safe to call concurrently.
   */
  Closeable acquire(String bundleName);

  /**
   * Submit a bundle download task, returning an {@link AsyncResource} that becomes ready when the task completes.
   * Closing the returned resource before completion cancels the task. The underlying executor bounds the concurrency
   * of on-demand load work, so callers may submit as many tasks as needed. The task must return a non-null value
   * (it may be a completion token if the work is purely a side effect), since {@link AsyncResource} cannot hold null.
   */
  <T> AsyncResource<T> submitDownload(Callable<T> task);
}
