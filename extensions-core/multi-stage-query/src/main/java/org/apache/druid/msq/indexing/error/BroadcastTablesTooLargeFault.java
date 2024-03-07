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

package org.apache.druid.msq.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.planner.JoinAlgorithm;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.Objects;

@JsonTypeName(BroadcastTablesTooLargeFault.CODE)
public class BroadcastTablesTooLargeFault extends BaseMSQFault
{
  static final String CODE = "BroadcastTablesTooLarge";

  private final long maxBroadcastTablesSize;

  @Nullable
  private final JoinAlgorithm configuredJoinAlgorithm;

  @JsonCreator
  public BroadcastTablesTooLargeFault(
      @JsonProperty("maxBroadcastTablesSize") final long maxBroadcastTablesSize,
      @Nullable @JsonProperty("configuredJoinAlgorithm") final JoinAlgorithm configuredJoinAlgorithm
  )
  {
    super(CODE, makeMessage(maxBroadcastTablesSize, configuredJoinAlgorithm));
    this.maxBroadcastTablesSize = maxBroadcastTablesSize;
    this.configuredJoinAlgorithm = configuredJoinAlgorithm;
  }

  @JsonProperty
  public long getMaxBroadcastTablesSize()
  {
    return maxBroadcastTablesSize;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public JoinAlgorithm getConfiguredJoinAlgorithm()
  {
    return configuredJoinAlgorithm;
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
    if (!super.equals(o)) {
      return false;
    }
    BroadcastTablesTooLargeFault that = (BroadcastTablesTooLargeFault) o;
    return maxBroadcastTablesSize == that.maxBroadcastTablesSize
           && configuredJoinAlgorithm == that.configuredJoinAlgorithm;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), maxBroadcastTablesSize, configuredJoinAlgorithm);
  }

  private static String makeMessage(final long maxBroadcastTablesSize, final JoinAlgorithm configuredJoinAlgorithm)
  {
    if (configuredJoinAlgorithm == null || configuredJoinAlgorithm == JoinAlgorithm.BROADCAST) {
      return StringUtils.format(
          "Size of broadcast tables in JOIN exceeds reserved memory limit "
          + "(memory reserved for broadcast tables = [%,d] bytes). "
          + "Increase available memory, or set [%s: %s] in query context to use a shuffle-based join.",
          maxBroadcastTablesSize,
          PlannerContext.CTX_SQL_JOIN_ALGORITHM,
          JoinAlgorithm.SORT_MERGE.toString()
      );
    } else {
      return StringUtils.format(
          "Size of broadcast tables in JOIN exceeds reserved memory limit "
          + "(memory reserved for broadcast tables = [%,d] bytes). "
          + "Try increasing available memory. "
          + "This query is using broadcast JOIN even though [%s: %s] is set in query context, because the configured "
          + "join algorithm does not support the join condition.",
          maxBroadcastTablesSize,
          PlannerContext.CTX_SQL_JOIN_ALGORITHM,
          configuredJoinAlgorithm.toString()
      );
    }
  }
}
