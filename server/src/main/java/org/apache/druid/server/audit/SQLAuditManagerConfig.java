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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.HumanReadableBytesRange;

/**
 */
public class SQLAuditManagerConfig implements AuditManagerConfig
{
  @JsonProperty
  private final long auditHistoryMillis = 7 * 24 * 60 * 60 * 1000L; // 1 WEEK

  @JsonProperty
  private final boolean includePayloadAsDimensionInMetric = false;

  @JsonProperty
  @HumanReadableBytesRange(
      min = -1,
      message = "maxPayloadSizeBytes must either be -1 (for disabling the check) or a non negative number"
  )
  private final HumanReadableBytes maxPayloadSizeBytes = HumanReadableBytes.valueOf(-1);

  @JsonProperty
  private final boolean skipNullField = false;

  public long getAuditHistoryMillis()
  {
    return auditHistoryMillis;
  }

  public boolean isIncludePayloadAsDimensionInMetric()
  {
    return includePayloadAsDimensionInMetric;
  }

  @Override
  public long getMaxPayloadSizeBytes()
  {
    return maxPayloadSizeBytes.getBytes();
  }

  @Override
  public boolean isSkipNullField()
  {
    return skipNullField;
  }

}
