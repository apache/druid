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

package org.apache.druid.segment.realtime;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Provides a way for the outside world to talk to objects in the indexing service. Handlers are held in an
 * in-memory registry on this host and reached through {@link ChatHandlerResource} via the task's known
 * {@link org.apache.druid.indexer.TaskLocation}
 */
public class LocalChatHandlerProvider implements ChatHandlerProvider
{
  private static final Logger log = new Logger(LocalChatHandlerProvider.class);

  private final ConcurrentMap<String, ChatHandler> handlers;

  @Inject
  public LocalChatHandlerProvider()
  {
    this.handlers = new ConcurrentHashMap<>();
  }

  @Override
  public void register(final String service, ChatHandler handler)
  {
    log.debug("Registering Eventhandler[%s]", service);

    if (handlers.putIfAbsent(service, handler) != null) {
      throw new ISE("handler already registered for service[%s]", service);
    }
  }

  @Override
  public void unregister(final String service)
  {
    log.debug("Unregistering chat handler[%s]", service);

    final ChatHandler handler = handlers.get(service);
    if (handler == null) {
      log.warn("handler[%s] not currently registered, ignoring.", service);
      return;
    }

    handlers.remove(service, handler);
  }

  @Override
  public Optional<ChatHandler> get(final String key)
  {
    return Optional.fromNullable(handlers.get(key));
  }
}
