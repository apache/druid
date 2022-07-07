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

package org.apache.druid.rpc;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;

/**
 * Used by {@link ServiceClient} to locate services. Thread-safe.
 */
public interface ServiceLocator extends Closeable
{
  /**
   * Returns a future that resolves to a set of {@link ServiceLocation}.
   *
   * If the returned object returns true from {@link ServiceLocations#isClosed()}, it means the service has closed
   * permanently. Otherwise, any of the returned locations in {@link ServiceLocations#getLocations()} is a viable
   * selection.
   *
   * It is possible for the list of locations to be empty. This means that the service is not currently available,
   * but also has not been closed, so it may become available at some point in the future.
   */
  ListenableFuture<ServiceLocations> locate();

  @Override
  void close();
}
