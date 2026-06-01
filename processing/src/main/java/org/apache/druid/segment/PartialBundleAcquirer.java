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

import com.google.common.util.concurrent.ListeningExecutorService;

import java.io.Closeable;

/**
 * Context object that bundles together pieces of cache-layer machinery needed to do on-demand partial segment loading:
 * <ol>
 *   <li>A hook for acquiring a hold on the cache-layer bundle.</li>
 *   <li>The executor on which partial downloads should run.</li>
 * </ol>
 */
public interface PartialBundleAcquirer
{
  /**
   * Acquire a hold on the cache-layer bundle with the given name. The returned {@link Closeable} releases the hold
   * when closed (the bundle itself stays in the cache for re-use by subsequent acquires).
   * <p>
   * Implementations should be safe to call concurrently for different bundle names.
   */
  Closeable acquire(String bundleName);

  /**
   * Executor which callers use to submit bundle download tasks. The executor is responsible for bounding the
   * concurrency of on-demand load work, so callers may submit as many tasks as needed.
   */
  ListeningExecutorService getDownloadExec();
}
