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

package org.apache.druid.indexing.kafka;

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.java.util.common.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Common place to keep all kafka consumer configs
 */
public class KafkaConsumerConfigs
{

  public static Map<String, Object> getConsumerProperties()
  {
    final Map<String, Object> props = new HashMap<>();
    props.put("metadata.max.age.ms", "10000");
    props.put("group.id", StringUtils.format("kafka-supervisor-%s", IdUtils.getRandomId()));
    props.put("auto.offset.reset", "none");
    props.put("enable.auto.commit", "false");
    props.put("isolation.level", "read_committed");
    return props;
  }

}
