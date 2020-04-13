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

package org.apache.druid.java.util.metrics;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.emitter.core.ParametrizedUriEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class ParametrizedUriEmitterMonitor extends FeedDefiningMonitor
{
  private final ParametrizedUriEmitter parametrizedUriEmitter;
  private final Map<URI, HttpPostEmitterMonitor> monitors = new HashMap<>();

  public ParametrizedUriEmitterMonitor(String feed, ParametrizedUriEmitter parametrizedUriEmitter)
  {
    super(feed);
    this.parametrizedUriEmitter = parametrizedUriEmitter;
  }

  private void updateMonitors()
  {
    parametrizedUriEmitter.forEachEmitter(
        (uri, emitter) -> {
          monitors.computeIfAbsent(
              uri,
              u -> {
                HttpPostEmitterMonitor monitor = new HttpPostEmitterMonitor(
                    feed,
                    emitter,
                    ImmutableMap.of("uri", uri.toString())
                );
                monitor.start();
                return monitor;
              }
          );
        }
    );
  }

  @Override
  public void stop()
  {
    monitors.values().forEach(AbstractMonitor::stop);
    super.stop();
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    updateMonitors();
    monitors.values().forEach(m -> m.doMonitor(emitter));
    return true;
  }
}
