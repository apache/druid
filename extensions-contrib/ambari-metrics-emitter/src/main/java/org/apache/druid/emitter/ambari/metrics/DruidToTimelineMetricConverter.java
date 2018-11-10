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

package org.apache.druid.emitter.ambari.metrics;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.hadoop.metrics2.sink.timeline.TimelineMetric;


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = WhiteListBasedDruidToTimelineEventConverter.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "all", value = SendAllTimelineEventConverter.class),
    @JsonSubTypes.Type(name = "whiteList", value = WhiteListBasedDruidToTimelineEventConverter.class)
})
public interface DruidToTimelineMetricConverter
{
  /**
   * This function acts as a filter. It returns <tt>null</tt> if the event is not suppose to be emitted to Ambari Server
   * Also This function will define the mapping between the druid event dimension's values and Ambari Metric Name
   *
   * @param serviceMetricEvent Druid event ot type {@link ServiceMetricEvent}
   *
   * @return {@link TimelineMetric} or <tt>null</tt>
   */
  TimelineMetric druidEventToTimelineMetric(ServiceMetricEvent serviceMetricEvent);
}
