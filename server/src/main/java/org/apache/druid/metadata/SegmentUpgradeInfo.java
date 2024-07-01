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

package org.apache.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Information about the id and upgraded from segment id created as a result of segment upgrades
 * upgradedFromSegmentId is null for segments that haven't been upgraded
 */
public class SegmentUpgradeInfo
{
  private final String id;
  private final String upgradedFromSegmentId;

  @JsonCreator
  public SegmentUpgradeInfo(
      @JsonProperty("id") String id,
      @JsonProperty("upgradedFromSegmentId") @Nullable String upgradedFromSegmentId
  )
  {
    this.id = id;
    this.upgradedFromSegmentId = upgradedFromSegmentId;
  }

  @JsonProperty
  public String getId()
  {
    return id;
  }

  @Nullable
  @JsonProperty
  public String getUpgradedFromSegmentId()
  {
    return upgradedFromSegmentId;
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
    SegmentUpgradeInfo that = (SegmentUpgradeInfo) o;
    return Objects.equals(id, that.id)
           && Objects.equals(upgradedFromSegmentId, that.upgradedFromSegmentId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(id, upgradedFromSegmentId);
  }
}
