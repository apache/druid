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

package org.apache.druid.audit;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;

import java.util.Objects;

/**
 * Represents a single audit event with serialized payload.
 */
public class AuditEvent
{
  private final String key;
  private final String type;
  private final AuditInfo auditInfo;
  private final DateTime auditTime;

  private final String serializedPayload;
  private final Object payload;

  private AuditEvent(
      String key,
      String type,
      AuditInfo authorInfo,
      DateTime auditTime,
      Object payload
  )
  {
    this(key, type, authorInfo, payload, null, auditTime);
  }

  public AuditEvent(
      String key,
      String type,
      AuditInfo authorInfo,
      String serializedPayload,
      DateTime auditTime
  )
  {
    this(key, type, authorInfo, null, serializedPayload, auditTime);
  }

  public AuditEvent(
      String key,
      String type,
      AuditInfo authorInfo,
      Object payload,
      String serializedPayload,
      DateTime auditTime
  )
  {
    Preconditions.checkNotNull(key, "key cannot be null");
    Preconditions.checkNotNull(type, "type cannot be null");
    Preconditions.checkNotNull(authorInfo, "author cannot be null");
    this.key = key;
    this.type = type;
    this.auditInfo = authorInfo;
    this.auditTime = auditTime == null ? DateTimes.nowUtc() : auditTime;
    this.serializedPayload = serializedPayload;
    this.payload = payload;
  }

  public String getKey()
  {
    return key;
  }

  public String getType()
  {
    return type;
  }

  public AuditInfo getAuditInfo()
  {
    return auditInfo;
  }

  public Object getPayload()
  {
    return payload;
  }

  public String getPayloadAsString()
  {
    return serializedPayload;
  }

  public DateTime getAuditTime()
  {
    return auditTime;
  }

  public static Builder builder()
  {
    return new Builder();
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

    AuditEvent that = (AuditEvent) o;
    return Objects.equals(this.auditTime, that.auditTime)
           && Objects.equals(this.key, that.key)
           && Objects.equals(this.type, that.type)
           && Objects.equals(this.auditInfo, that.auditInfo)
           && Objects.equals(this.payload, that.payload)
           && Objects.equals(this.serializedPayload, that.serializedPayload);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(key, type, auditInfo, payload, serializedPayload, auditTime);
  }

  public static class Builder
  {
    private String key;
    private String type;
    private AuditInfo auditInfo;

    private String serializedPayload;
    private Object payload;

    private DateTime auditTime;

    private Builder()
    {
      this.key = null;
      this.auditInfo = null;
      this.serializedPayload = null;
      this.auditTime = DateTimes.nowUtc();
    }

    public Builder key(String key)
    {
      this.key = key;
      return this;
    }

    public Builder type(String type)
    {
      this.type = type;
      return this;
    }

    public Builder auditInfo(AuditInfo auditInfo)
    {
      this.auditInfo = auditInfo;
      return this;
    }

    public Builder payloadAsString(String serializedPayload)
    {
      this.serializedPayload = serializedPayload;
      return this;
    }

    public Builder payload(Object payload)
    {
      this.payload = payload;
      return this;
    }

    public Builder auditTime(DateTime auditTime)
    {
      this.auditTime = auditTime;
      return this;
    }

    public AuditEvent build()
    {
      if (payload != null) {
        return new AuditEvent(key, type, auditInfo, auditTime, payload);
      } else {
        return new AuditEvent(key, type, auditInfo, serializedPayload, auditTime);
      }
    }
  }
}
