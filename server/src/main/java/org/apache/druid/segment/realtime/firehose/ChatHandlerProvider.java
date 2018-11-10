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

package org.apache.druid.segment.realtime.firehose;

import com.google.common.base.Optional;

/**
 */
public interface ChatHandlerProvider
{
  /**
   * Registers a chat handler which provides an API for others to talk to objects in the indexing service. Depending
   * on the implementation, this method may also announce this node so that it can be discovered by other services.
   *
   * @param key     a unique name identifying this service
   * @param handler instance which implements the API to be exposed
   */
  void register(String key, ChatHandler handler);

  /**
   * Registers a chat handler which provides an API for others to talk to objects in the indexing service. Setting
   * announce to false instructs the implementation to only register the handler to expose the API and skip any
   * discovery announcements that might have been broadcast.
   *
   * @param key      a unique name identifying this service
   * @param handler  instance which implements the API to be exposed
   * @param announce for implementations that have a service discovery mechanism, whether this node should be announced
   */
  void register(String key, ChatHandler handler, boolean announce);

  /**
   * Unregisters a chat handler.
   *
   * @param key the name of the service
   */
  void unregister(String key);

  /**
   * Retrieves a chat handler.
   *
   * @param key the name of the service
   */
  Optional<ChatHandler> get(String key);
}
