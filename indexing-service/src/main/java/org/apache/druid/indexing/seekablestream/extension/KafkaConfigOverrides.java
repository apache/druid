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

package org.apache.druid.indexing.seekablestream.extension;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.guice.annotations.ExtensionPoint;

import java.util.Map;

/**
 * This is used to allow extensions to make adjustments to the Kafka consumer properties.
 *
 * This interface is only used by the druid-kafka-indexing-service extension, but the interface definition must
 * be placed in a non-extension module in order for other extensions to be able to provide subtypes visible within
 * the druid-kafka-indexing-service extension.
 */
@ExtensionPoint
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface KafkaConfigOverrides
{
  /**
   * Given a map of Kafka consumer properties, return a new potentially adjusted map of properties.
   *
   * @param originalConsumerProperties Kafka consumer properties
   * @return Adjusted copy of Kafka consumer properties
   */
  Map<String, Object> overrideConfigs(Map<String, Object> originalConsumerProperties);
}
