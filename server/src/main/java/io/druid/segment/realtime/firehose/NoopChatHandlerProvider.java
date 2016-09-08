/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.realtime.firehose;

import com.google.common.base.Optional;

/**
 */
public class NoopChatHandlerProvider implements ChatHandlerProvider
{
  @Override
  public void register(String key, ChatHandler handler)
  {
    // do nothing
  }

  @Override
  public void register(String key, ChatHandler handler, boolean announce)
  {
    // do nothing
  }

  @Override
  public void unregister(String key)
  {
    // do nothing
  }

  @Override
  public Optional<ChatHandler> get(String key)
  {
    return Optional.absent();
  }
}
