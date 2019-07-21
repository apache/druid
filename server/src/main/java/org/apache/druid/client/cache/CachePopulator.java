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

package org.apache.druid.client.cache;

import org.apache.druid.java.util.common.guava.Sequence;

import java.util.function.Function;

/**
 * Abstraction of mechanism for populating a {@link Cache} by wrapping a {@link Sequence} and providing a function to
 * extract the values from it. At runtime, the {@link CachePopulator} implementation is used as a singleton and
 * injected where needed to share between all cacheables, which requires the {@link Cache} itself to be thread-safe.
 *
 * Consumers of the {@link Sequence} will either be a processing thread (in the case of a historical or task), or
 * an http thread in the case of a broker. See:
 *
 *  historicals:    {@link org.apache.druid.server.coordination.ServerManager} and
 *                  {@link org.apache.druid.client.CachingQueryRunner}
 *
 *  realtime tasks: {@link org.apache.druid.segment.realtime.appenderator.SinkQuerySegmentWalker} and
 *                  {@link org.apache.druid.client.CachingQueryRunner}
 *
 *  brokers:        {@link org.apache.druid.server.ClientQuerySegmentWalker} and
 *                  {@link org.apache.druid.client.CachingClusteredClient}
 *
 *  for additional details
 */
public interface CachePopulator
{
  <T, CacheType> Sequence<T> wrap(
      Sequence<T> sequence,
      Function<T, CacheType> cacheFn,
      Cache cache,
      Cache.NamedKey cacheKey
  );
}
