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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.audit.AuditEvent;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;

import java.util.Objects;

/**
 * Record of an audit event persisted in the metadata store.
 */
public class AuditRecord
{
  private final String key;
  private final String type;
  private final AuditInfo auditInfo;
  private final String payload;
  private final DateTime auditTime;

  public static AuditRecord fromAuditEvent(AuditEvent event)
  {
    return new AuditRecord(
        event.getKey(),
        event.getType(),
        event.getAuditInfo(),
        event.getPayloadAsString(),
        event.getAuditTime()
    );
  }

  @JsonCreator
  public AuditRecord(
      @JsonProperty("key") String key,
      @JsonProperty("type") String type,
      @JsonProperty("auditInfo") AuditInfo authorInfo,
      @JsonProperty("payload") String payload,
      @JsonProperty("auditTime") DateTime auditTime
  )
  {
    Preconditions.checkNotNull(key, "key cannot be null");
    Preconditions.checkNotNull(type, "type cannot be null");
    Preconditions.checkNotNull(authorInfo, "author cannot be null");
    this.key = key;
    this.type = type;
    this.auditInfo = authorInfo;
    this.auditTime = auditTime == null ? DateTimes.nowUtc() : auditTime;
    this.payload = payload == null ? "" : payload;
  }

  @JsonProperty
  public String getKey()
  {
    return key;
  }

  @JsonProperty
  public String getType()
  {
    return type;
  }

  @JsonProperty
  public AuditInfo getAuditInfo()
  {
    return auditInfo;
  }

  /**
   * @return Payload as a non-null String.
   */
  @JsonProperty
  public String getPayload()
  {
    return payload;
  }

  /**
   * @return audit time as DateTime
   */
  @JsonProperty
  public DateTime getAuditTime()
  {
    return auditTime;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AuditRecord that = (AuditRecord) o;
    return Objects.equals(this.auditTime, that.auditTime)
           && Objects.equals(this.key, that.key)
           && Objects.equals(this.type, that.type)
           && Objects.equals(this.auditInfo, that.auditInfo)
           && Objects.equals(this.payload, that.payload);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(key, type, auditInfo, payload, auditTime);
  }

}
