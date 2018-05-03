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

package io.druid.emitter.statsd;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.timgroup.statsd.StatsDClient;
import io.druid.java.util.emitter.core.Event;

import java.util.Map;
/**
 * Interface to allow implementations to define their custom way of handling statsd events.
 * Users will need to set druid.emitter.statsd.eventHandler property to choose appropriate event handler.
 * Following is the behavior for setting this property
 * 1. druid.emitter.statsd.eventHandler not defined - DefaultStatsDEventHandler will be used
 * 2. druid.emitter.statsd.eventHandler set to {"type": "default"} - DefaultStatsDEventHandler will be used
 * 3. druid.emitter.statsd.eventHandler set to {"type": something_which_does_not_exist} - DefaultStatsDEventHandler will be used
 * 4. druid.emitter.statsd.eventHandler set to {"type": type_name_of_custom_handler} - Custom handler will be used
 * */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = DefaultStatsDEventHandler.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "default", value = DefaultStatsDEventHandler.class)
})
public interface StatsDEventHandler
{
  /**
   * This method should is responsible to extract information from the Druid {@link Event} in a custom way
   * and use that information to perform actions like populating appropriate metrics in the StatsD client
   *
   * @param statsDClient StatsD client
   * @param event        An {@link Event} emitted by Druid
   * @param config       {@link StatsDEmitterConfig}
   * @param filterMap    Map from Druid metric names to {@link StatsDDimension} used for whitelisting druid metrics to emit,
   *                     dimensions to emit for the metric and figuring out StatsD stat type.
   */
  void handleEvent(
      StatsDClient statsDClient,
      Event event,
      StatsDEmitterConfig config,
      Map<String, StatsDDimension> filterMap
  );
}
