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

package org.apache.druid.server.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.audit.AuditEvent;
import org.apache.druid.audit.AuditManagerConfig;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.JsonNonNull;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;

import java.io.IOException;

public class AuditSerdeHelper implements AuditEvent.PayloadDeserializer
{
  /**
   * Default message stored instead of the actual audit payload if the audit
   * payload size exceeds the maximum size limit.
   */
  private static final String PAYLOAD_TRUNCATED_MSG =
      "Payload truncated as it exceeds 'druid.audit.manager.maxPayloadSizeBytes'";

  private final ObjectMapper jsonMapper;
  private final ObjectMapper jsonMapperSkipNulls;

  private final AuditManagerConfig config;

  @Inject
  public AuditSerdeHelper(
      AuditManagerConfig config,
      @Json ObjectMapper jsonMapper,
      @JsonNonNull ObjectMapper jsonMapperSkipNulls
  )
  {
    this.config = config;
    this.jsonMapper = jsonMapper;
    this.jsonMapperSkipNulls = jsonMapperSkipNulls;
  }

  public AuditRecord processAuditEvent(AuditEvent event)
  {
    final String serialized = event.getPayloadAsString() == null
                              ? serializePayloadToString(event.getPayload())
                              : event.getPayloadAsString();

    return new AuditRecord(
        event.getKey(),
        event.getType(),
        event.getAuditInfo(),
        truncateSerializedAuditPayload(serialized),
        event.getAuditTime()
    );
  }

  @Override
  public <T> T deserializePayloadFromString(String serializedPayload, Class<T> clazz)
  {
    if (serializedPayload == null || serializedPayload.isEmpty()) {
      return null;
    } else if (serializedPayload.contains(PAYLOAD_TRUNCATED_MSG)) {
      throw InvalidInput.exception("Cannot deserialize audit payload[%s].", serializedPayload);
    }

    try {
      return jsonMapper.readValue(serializedPayload, clazz);
    }
    catch (IOException e) {
      throw InvalidInput.exception(
          e,
          "Could not deserialize audit payload[%s] into class[%s]",
          serializedPayload, clazz
      );
    }
  }

  private String serializePayloadToString(Object payload)
  {
    if (payload == null) {
      return "";
    }

    try {
      return config.isSkipNullField()
             ? jsonMapperSkipNulls.writeValueAsString(payload)
             : jsonMapper.writeValueAsString(payload);
    }
    catch (IOException e) {
      throw new ISE(e, "Could not serialize audit payload[%s]", payload);
    }
  }

  /**
   * Truncates the audit payload string if it exceeds
   * {@link AuditManagerConfig#getMaxPayloadSizeBytes()}.
   */
  private String truncateSerializedAuditPayload(String serializedPayload)
  {
    if (serializedPayload == null || config.getMaxPayloadSizeBytes() < 0) {
      return serializedPayload;
    }

    int payloadSize = serializedPayload.getBytes().length;
    if (payloadSize > config.getMaxPayloadSizeBytes()) {
      return PAYLOAD_TRUNCATED_MSG + StringUtils.format("[%s].", config.getMaxPayloadSizeBytes());
    } else {
      return serializedPayload;
    }
  }
}
