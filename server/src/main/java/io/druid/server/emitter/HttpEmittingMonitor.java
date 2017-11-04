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

package io.druid.server.emitter;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.HttpPostEmitter;
import com.metamx.emitter.core.ParametrizedUriEmitter;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.metrics.AbstractMonitor;
import com.metamx.metrics.FeedDefiningMonitor;
import com.metamx.metrics.HttpPostEmitterMonitor;
import com.metamx.metrics.ParametrizedUriEmitterMonitor;

/**
 * Able to monitor {@link HttpPostEmitter} or {@link ParametrizedUriEmitter}, which is based on the former.
 */
public class HttpEmittingMonitor extends AbstractMonitor
{
  private AbstractMonitor delegate;

  @Inject
  public HttpEmittingMonitor(Emitter emitter)
  {
    if (emitter instanceof HttpPostEmitter) {
      delegate = new HttpPostEmitterMonitor(
          FeedDefiningMonitor.DEFAULT_METRICS_FEED,
          (HttpPostEmitter) emitter,
          ImmutableMap.of()
      );
    } else if (emitter instanceof ParametrizedUriEmitter) {
      delegate = new ParametrizedUriEmitterMonitor(
          FeedDefiningMonitor.DEFAULT_METRICS_FEED,
          (ParametrizedUriEmitter) emitter
      );
    } else {
      throw new IllegalStateException(
          "Unable to use HttpEmittingMonitor with emitter other than HttpPostEmitter or ParametrizedUriEmitter, " +
          emitter.getClass() + " is used"
      );
    }
  }

  @Override
  public boolean doMonitor(ServiceEmitter serviceEmitter)
  {
    return delegate.doMonitor(serviceEmitter);
  }
}
