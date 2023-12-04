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
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.JsonNonNull;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class AuditSerdeHelper
{
  /**
   * Default message stored instead of the actual audit payload if the audit
   * payload size exceeds the maximum size limit.
   */
  private static final String PAYLOAD_TRUNCATED_MSG =
      "Payload truncated as it exceeds 'druid.audit.manager.maxPayloadSizeBytes'";
  private static final String SERIALIZE_ERROR_MSG =
      "Error serializing payload";
  private static final Logger log = new Logger(AuditSerdeHelper.class);

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
      // Do not throw exception, only log error
      log.error(e, "Could not serialize audit payload[%s]", payload);
      return SERIALIZE_ERROR_MSG;
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

    int payloadSize = serializedPayload.getBytes(StandardCharsets.UTF_8).length;
    if (payloadSize > config.getMaxPayloadSizeBytes()) {
      return PAYLOAD_TRUNCATED_MSG + StringUtils.format("[%s].", config.getMaxPayloadSizeBytes());
    } else {
      return serializedPayload;
    }
  }
}
