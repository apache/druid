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
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.JsonNonNull;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.Escalator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Audit utility class that can be used by different implementations of
 * {@link org.apache.druid.audit.AuditManager} to serialize/deserialize audit
 * payloads based on the values configured in {@link AuditManagerConfig}.
 */
public class AuditSerdeHelper
{
  /**
   * Default message stored instead of the actual audit payload if the audit
   * payload size exceeds the maximum size limit.
   */
  private static final String PAYLOAD_TRUNCATED_MSG =
      "Payload truncated as it exceeds 'druid.audit.manager.maxPayloadSizeBytes'";
  private static final String SERIALIZE_ERROR_MSG =
      "Error serializing payload. Check logs for details.";
  private static final Logger log = new Logger(AuditSerdeHelper.class);

  private final ObjectMapper jsonMapper;
  private final ObjectMapper jsonMapperSkipNulls;

  private final String systemIdentity;
  private final AuditManagerConfig config;

  @Inject
  public AuditSerdeHelper(
      AuditManagerConfig config,
      Escalator escalator,
      @Json ObjectMapper jsonMapper,
      @JsonNonNull ObjectMapper jsonMapperSkipNulls
  )
  {
    this.config = config;
    this.jsonMapper = jsonMapper;
    this.jsonMapperSkipNulls = jsonMapperSkipNulls;
    this.systemIdentity = escalator == null
                          ? null : escalator.createEscalatedAuthenticationResult().getIdentity();
  }

  /**
   * Checks if the given audit event needs to be handled.
   *
   * @return true only if the event was not initiated by the Druid system OR if
   * system requests should be audited too.
   */
  public boolean shouldProcessAuditEntry(AuditEntry entry)
  {
    final boolean isSystemRequest = systemIdentity != null
                                    && systemIdentity.equals(entry.getAuditInfo().getIdentity());
    return config.isAuditSystemRequests() || !isSystemRequest;
  }

  /**
   * Processes the given AuditEntry for further use such as logging or persistence.
   * This involves serializing and truncating the payload based on the values
   * configured in {@link AuditManagerConfig}.
   *
   * @return A new AuditEntry with a serialized payload that can be used for
   * logging or persistence.
   */
  public AuditEntry processAuditEntry(AuditEntry entry)
  {
    final AuditEntry.Payload payload = entry.getPayload();
    final String serialized = payload.serialized() == null
                              ? serializePayloadToString(payload.raw())
                              : payload.serialized();

    final AuditEntry.Payload processedPayload = AuditEntry.Payload.fromString(
        truncateSerializedAuditPayload(serialized)
    );
    return new AuditEntry(
        entry.getKey(),
        entry.getType(),
        entry.getAuditInfo(),
        entry.getRequest(),
        processedPayload,
        entry.getAuditTime()
    );
  }

  private String serializePayloadToString(Object rawPayload)
  {
    if (rawPayload == null) {
      return "";
    }

    try {
      return config.isSkipNullField()
             ? jsonMapperSkipNulls.writeValueAsString(rawPayload)
             : jsonMapper.writeValueAsString(rawPayload);
    }
    catch (IOException e) {
      // Do not throw exception, only log error
      log.error(e, "Could not serialize audit payload[%s]", rawPayload);
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
