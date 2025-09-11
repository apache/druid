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

package org.apache.druid.sql.avatica;

import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.Timer;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.MetricsHelper;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.MetricsAwareAvaticaHandler;
import org.apache.calcite.avatica.util.UnsynchronizedBuffer;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.server.DruidNode;
import org.eclipse.jetty.server.Handler;

public abstract class DruidAvaticaHandler extends Handler.Abstract implements MetricsAwareAvaticaHandler
{
  protected final Service service;
  protected final MetricsSystem metrics;
  protected final Timer requestTimer;
  protected final ThreadLocal<UnsynchronizedBuffer> threadLocalBuffer;

  protected DruidAvaticaHandler(
      final DruidMeta druidMeta,
      final AvaticaMonitor avaticaMonitor,
      final Class<?> timerClass
  )
  {
    this.service = new LocalService(druidMeta);
    this.metrics = avaticaMonitor;
    this.threadLocalBuffer = ThreadLocal.withInitial(UnsynchronizedBuffer::new);
    this.requestTimer = this.metrics.getTimer(
        MetricsHelper.concat(timerClass, MetricsAwareAvaticaHandler.REQUEST_TIMER_NAME)
    );
  }

  @Override
  public MetricsSystem getMetrics()
  {
    return metrics;
  }

  @Override
  public void setServerRpcMetadata(Service.RpcMetadataResponse metadata)
  {
    service.setRpcMetadata(metadata);
  }
}
