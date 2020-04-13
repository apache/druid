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

package org.apache.druid.query.lookup;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.metrics.DataSourceTaskIdHolder;

class LookupListeningAnnouncerConfig
{
  public static final String DEFAULT_TIER = "__default";
  private final DataSourceTaskIdHolder dataSourceTaskIdHolder;
  @JsonProperty("lookupTier")
  private String lookupTier = null;
  @JsonProperty("lookupTierIsDatasource")
  private boolean lookupTierIsDatasource = false;

  @JsonCreator
  public LookupListeningAnnouncerConfig(
      @JacksonInject DataSourceTaskIdHolder dataSourceTaskIdHolder
  )
  {
    this.dataSourceTaskIdHolder = dataSourceTaskIdHolder;
  }

  public String getLookupTier()
  {
    Preconditions.checkArgument(
        !(lookupTierIsDatasource && null != lookupTier),
        "Cannot specify both `lookupTier` and `lookupTierIsDatasource`"
    );
    final String lookupTier = lookupTierIsDatasource ? dataSourceTaskIdHolder.getDataSource() : this.lookupTier;

    return Preconditions.checkNotNull(
        lookupTier == null ? DEFAULT_TIER : StringUtils.emptyToNullNonDruidDataString(lookupTier),
        "Cannot have empty lookup tier from %s",
        lookupTierIsDatasource ? "bound value" : LookupModule.PROPERTY_BASE
    );
  }
}
