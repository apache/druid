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
import org.apache.druid.common.config.Configs;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.HumanReadableBytesRange;

public class LoggingAuditManagerConfig implements AuditManagerConfig
{
  @JsonProperty
  private final AuditLogger.Level logLevel;

  @JsonProperty
  @HumanReadableBytesRange(
      min = -1,
      message = "maxPayloadSizeBytes must either be -1 (for disabling the check) or a non negative number"
  )
  private final HumanReadableBytes maxPayloadSizeBytes;

  @JsonProperty
  private final boolean skipNullField;

  @JsonProperty
  private final boolean auditSystemRequests;

  @JsonCreator
  public LoggingAuditManagerConfig(
      @JsonProperty("logLevel") AuditLogger.Level logLevel,
      @JsonProperty("auditSystemRequests") Boolean auditSystemRequests,
      @JsonProperty("maxPayloadSizeBytes") HumanReadableBytes maxPayloadSizeBytes,
      @JsonProperty("skipNullField") Boolean skipNullField
  )
  {
    this.logLevel = Configs.valueOrDefault(logLevel, AuditLogger.Level.INFO);
    this.auditSystemRequests = Configs.valueOrDefault(auditSystemRequests, true);
    this.maxPayloadSizeBytes = Configs.valueOrDefault(maxPayloadSizeBytes, HumanReadableBytes.valueOf(-1));
    this.skipNullField = Configs.valueOrDefault(skipNullField, false);
  }

  @Override
  public boolean isSkipNullField()
  {
    return skipNullField;
  }

  @Override
  public long getMaxPayloadSizeBytes()
  {
    return maxPayloadSizeBytes.getBytes();
  }

  @Override
  public boolean isAuditSystemRequests()
  {
    return auditSystemRequests;
  }

  public AuditLogger.Level getLogLevel()
  {
    return logLevel;
  }
}
