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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;

import java.util.Objects;

/**
 * Serializable record of an audit event that can be persisted, logged or sent
 * over REST APIs.
 */
public class AuditEntry
{
  private final String key;
  private final String type;
  private final AuditInfo auditInfo;
  private final RequestInfo request;
  private final Payload payload;
  private final DateTime auditTime;

  @JsonCreator
  public AuditEntry(
      @JsonProperty("key") String key,
      @JsonProperty("type") String type,
      @JsonProperty("auditInfo") AuditInfo authorInfo,
      @JsonProperty("request") RequestInfo request,
      @JsonProperty("payload") Payload payload,
      @JsonProperty("auditTime") DateTime auditTime
  )
  {
    Preconditions.checkNotNull(key, "key cannot be null");
    Preconditions.checkNotNull(type, "type cannot be null");
    Preconditions.checkNotNull(authorInfo, "author cannot be null");
    this.key = key;
    this.type = type;
    this.auditInfo = authorInfo;
    this.request = request;
    this.auditTime = auditTime == null ? DateTimes.nowUtc() : auditTime;
    this.payload = payload == null ? Payload.fromString("") : payload;
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
   * Details of the REST API request associated with this audit, if any.
   */
  @JsonProperty
  public RequestInfo getRequest()
  {
    return request;
  }

  /**
   * Non-null payload of the audit event.
   */
  @JsonProperty
  public Payload getPayload()
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

    AuditEntry that = (AuditEntry) o;
    return Objects.equals(this.auditTime, that.auditTime)
           && Objects.equals(this.key, that.key)
           && Objects.equals(this.type, that.type)
           && Objects.equals(this.auditInfo, that.auditInfo)
           && Objects.equals(this.request, that.request)
           && Objects.equals(this.payload, that.payload);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(key, type, auditInfo, request, payload, auditTime);
  }

  public static class Builder
  {
    private String key;
    private String type;
    private AuditInfo auditInfo;
    private RequestInfo requestInfo;
    private Object payload;
    private String serializedPayload;

    private DateTime auditTime;

    private Builder()
    {
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

    public Builder request(RequestInfo requestInfo)
    {
      this.requestInfo = requestInfo;
      return this;
    }

    public Builder serializedPayload(String serializedPayload)
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

    public AuditEntry build()
    {
      if (payload != null && serializedPayload != null) {
        throw DruidException.defensive(
            "Either payload[%s] or serializedPayload[%s] must be specified, not both.",
            payload, serializedPayload
        );
      }

      return new AuditEntry(key, type, auditInfo, requestInfo, new Payload(serializedPayload, payload), auditTime);
    }
  }

  /**
   * Payload of an {@link AuditEntry} that may be specified {@link #raw()} or {@link #serialized()}.
   */
  public static class Payload
  {
    private final String serialized;
    private final Object raw;

    @JsonCreator
    public static Payload fromString(String serialized)
    {
      return new Payload(serialized, null);
    }

    @JsonValue
    @Override
    public String toString()
    {
      return serialized == null ? "" : serialized;
    }

    private Payload(String serialized, Object raw)
    {
      this.serialized = serialized;
      this.raw = raw;
    }

    public String serialized()
    {
      return serialized;
    }

    public Object raw()
    {
      return raw;
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

      Payload that = (Payload) o;
      return Objects.equals(this.serialized, that.serialized)
             && Objects.equals(this.raw, that.raw);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(serialized, raw);
    }
  }
}
