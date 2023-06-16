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

package org.apache.druid.server.metrics;

import javax.inject.Inject;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.server.coordinator.DruidCoordinator;

/**
 * Monitor Coordinator running status.
 */
public class CoordinatorStatusMonitor extends AbstractMonitor {

  private final DruidCoordinator coordinator;

  @Inject
  public CoordinatorStatusMonitor(DruidCoordinator coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter) {
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();

    builder.setDimension("serviceType", "coordinator");
    emitter.emit(builder.build("leader/count", coordinator.isLeader() ? 1 : 0));

    return true;
  }
}
