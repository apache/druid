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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.hash.Hashing;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.indexing.DataSchema;

import java.util.Map;
import java.util.TreeMap;

public final class ShareGroupIngestionSpecFingerprint
{
  private ShareGroupIngestionSpecFingerprint()
  {
  }

  public static String compute(
      ObjectMapper mapper,
      DataSchema dataSchema,
      KafkaIndexTaskTuningConfig tuningConfig,
      ShareGroupIndexTaskIOConfig ioConfig
  )
  {
    final Map<String, Object> behavior = new TreeMap<>();
    behavior.put("dataSchema", dataSchema);
    behavior.put("groupId", ioConfig.getGroupId());
    behavior.put("inputFormat", ioConfig.getInputFormat());
    behavior.put("topic", ioConfig.getTopic());
    behavior.put("tuningConfig", tuningConfig);
    try {
      final byte[] canonical = mapper.writer()
                                     .with(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                                     .writeValueAsBytes(behavior);
      return Hashing.sha256().hashBytes(canonical).toString();
    }
    catch (JsonProcessingException e) {
      throw new ISE(e, "Unable to fingerprint share-group ingestion spec");
    }
  }
}
