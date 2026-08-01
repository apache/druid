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

import com.google.inject.Inject;
import org.apache.druid.client.SegmentAvailabilityTracker;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;

/**
 * Reports how many segments this Broker believes should be available but has no server for.
 * <p>
 * A standing gauge rather than a per-query count, so that operators can see the condition even when no query happens
 * to touch the affected segments. See apache/druid#18716.
 */
@LoadScope(roles = {NodeRole.BROKER_JSON_NAME})
public class SegmentAvailabilityMonitor extends AbstractMonitor
{
  private final SegmentAvailabilityTracker availabilityTracker;

  @Inject
  public SegmentAvailabilityMonitor(SegmentAvailabilityTracker availabilityTracker)
  {
    this.availabilityTracker = availabilityTracker;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    emitter.emit(
        ServiceMetricEvent.builder()
                          .setMetric("segment/noServer/count", availabilityTracker.getNumUnavailableSegments())
    );
    emitter.emit(
        ServiceMetricEvent.builder()
                          .setMetric("segment/noServer/tracked", availabilityTracker.getNumTrackedSegments())
    );
    return true;
  }
}
