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

package org.apache.druid.java.util.http.client.pool;

import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

public class MetricsEmittingResourcePool<K, V> extends ResourcePool<K, V>
{
  private final ServiceEmitter emitter;

  public MetricsEmittingResourcePool(ResourceFactory factory, ResourcePoolConfig config, boolean eagerInitialization, ServiceEmitter emitter)
  {
    super(factory, config, eagerInitialization);
    this.emitter = emitter;
  }

  @Override
  public ResourceContainer<V> take(final K key)
  {
    long startTime = System.nanoTime();
    ResourceContainer<V> retVal = super.take(key);
    long totalduration = System.nanoTime() - startTime;
    emitter.emit(ServiceMetricEvent.builder().setDimension("destination", key.toString()).setMetric("httpClient/channelAcquire/time", totalduration));
    return retVal;
  }

}
