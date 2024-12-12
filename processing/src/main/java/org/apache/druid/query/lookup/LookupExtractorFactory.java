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

package org.apache.druid.query.lookup;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Supplier;

import javax.annotation.Nullable;
import java.util.concurrent.TimeoutException;

/**
 * Users of Lookup Extraction need to implement a {@link LookupExtractorFactory} supplier of type {@link LookupExtractor}.
 * Such factory will manage the state and life cycle of an given lookup.
 * If a LookupExtractorFactory wishes to support idempotent updates, it needs to implement the  `replaces` method
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface LookupExtractorFactory extends Supplier<LookupExtractor>
{
  /**
   * <p>
   *   This method will be called to start the LookupExtractor upon registered
   *   Calling start multiple times should return true if successfully started.
   * </p>
   *
   * @return Returns false if is not successfully started the {@link LookupExtractor} otherwise returns true.
   */
  boolean start();

  /**
   * <p>
   *   This method will be called to stop the LookupExtractor upon Druid process stop. This would be used, for
   *   example, to stop any thread pools it might have.
   *   Calling this method multiple times should always return true if successfully closed.
   * </p>
   * @return Returns false if not successfully closed the {@link LookupExtractor} otherwise returns true
   */
  boolean close();

  /**
   * <p>
   *   This method will be called to drop the LookupExtractor upon explicit user request to coordinator to drop
   *   this lookup. In this method user can do additional cleanup (e.g. deleting disk persisted cache) not done simply
   *   when Druid process was being stopped to be restarted.
   *   Calling this method multiple times should always return true if successfully destroyed.
   * </p>
   * @return Returns false if not successfully destroyed the {@link LookupExtractor} otherwise returns true
   */
  default boolean destroy()
  {
    return close();
  }

  /**
   * This method is deprecated and is not removed only to allow 0.10.0 to 0.10.1 transition. It is not used
   * on a cluster that is running 0.10.1. It will be removed in a later release.
   */
  @Deprecated
  boolean replaces(@Nullable LookupExtractorFactory other);

  /**
   * @return Returns the actual introspection request handler, can return {@code null} if it is not supported.
   * This will be called once per HTTP request to introspect the actual lookup.
   */
  @Nullable
  LookupIntrospectHandler getIntrospectHandler();

  /**
   * awaitToInitialise blocks and wait for the cache to initialize fully.
   */
  void awaitInitialization() throws InterruptedException, TimeoutException;

  /**
   * @return true if cache is loaded and lookup is queryable else returns false
   */
  boolean isInitialized();
}
