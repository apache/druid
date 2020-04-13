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

package org.apache.druid.emitter.graphite;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = WhiteListBasedConverter.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "all", value = SendAllGraphiteEventConverter.class),
    @JsonSubTypes.Type(name = "whiteList", value = WhiteListBasedConverter.class)
})

public interface DruidToGraphiteEventConverter
{
  /**
   * This function acts as a filter. It returns <tt>null</tt> if the event is not suppose to be emitted to Graphite
   * Also This function will define the mapping between the druid event dimension's values and Graphite metric Path
   *
   * @param serviceMetricEvent Druid event ot type {@link ServiceMetricEvent}
   *
   * @return {@link GraphiteEvent} or <tt>null</tt>
   */
  GraphiteEvent druidEventToGraphite(ServiceMetricEvent serviceMetricEvent);
}
