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

package io.druid.server.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;

public class EventReceiverFirehoseRegister
{

  private static final Logger log = new Logger(EventReceiverFirehoseRegister.class);

  private final ConcurrentMap<String, EventReceiverFirehoseMetric> metrics = new ConcurrentHashMap<>();

  public void register(String serviceName, EventReceiverFirehoseMetric metric)
  {
    log.info("Registering EventReceiverFirehoseMetric for service [%s]", serviceName);
    if (metrics.putIfAbsent(serviceName, metric) != null) {
      throw new ISE("Service [%s] is already registered!", serviceName);
    }
  }

  public Iterable<Map.Entry<String, EventReceiverFirehoseMetric>> getMetrics()
  {
    return metrics.entrySet();
  }

  public void unregister(String serviceName)
  {
    log.info("Unregistering EventReceiverFirehoseMetric for service [%s]", serviceName);
    if (metrics.remove(serviceName) == null) {
      log.warn("Unregistering a non-exist service. Service [%s] never exists.", serviceName);
    }
  }
}
