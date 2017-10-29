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

package io.druid.audit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;

/**
 * An Entry in Audit Table.
 */
public class AuditEntry
{
  private final String key;
  private final String type;
  private final AuditInfo auditInfo;
  private final String payload;
  private final DateTime auditTime;

  @JsonCreator
  public AuditEntry(
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
    this.payload = payload;
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
  * @return returns payload as String
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

  public static AuditEntry.Builder builder()
  {
    return new AuditEntry.Builder();
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

    AuditEntry entry = (AuditEntry) o;

    if (!auditTime.equals(entry.auditTime)) {
      return false;
    }
    if (!auditInfo.equals(entry.auditInfo)) {
      return false;
    }
    if (!key.equals(entry.key)) {
      return false;
    }
    if (!payload.equals(entry.payload)) {
      return false;
    }
    if (!type.equals(entry.type)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = key.hashCode();
    result = 31 * result + type.hashCode();
    result = 31 * result + auditInfo.hashCode();
    result = 31 * result + payload.hashCode();
    result = 31 * result + auditTime.hashCode();
    return result;
  }

  public static class Builder
  {
    private String key;
    private String type;
    private AuditInfo auditInfo;
    private String payload;
    private DateTime auditTime;

    private Builder()
    {
      this.key = null;
      this.auditInfo = null;
      this.payload = null;
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

    public Builder payload(String payload)
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
      return new AuditEntry(key, type, auditInfo, payload, auditTime);
    }

  }

}
