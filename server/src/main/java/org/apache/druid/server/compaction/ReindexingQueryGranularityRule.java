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

package org.apache.druid.server.compaction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A {@link ReindexingRule} that specifies a query granularity for reindexing tasks to configure.
 * <p>
 * This rule controls the granularity of individual rows written to segments. For example, changing from
 * minute-level query granularity to hour-level query granularity can reduce data size and improve query performance
 * for older data that doesn't require fine-grained time resolution.
 * <p>
 * This is a non-additive rule. Multiple query granularity rules cannot be applied to the same segment.
 * <p>
 * Example inline usage:
 * <pre>{@code
 * {
 *     "id": "hour-30d",
 *     "olderThan": "P30D",
 *     "queryGranularity": "HOUR"
 *     "rollup": true,
 *     "description": "Rollup to hour query granularity for data older than 30 days"
 * }
 * }</pre>
 */
public class ReindexingQueryGranularityRule extends AbstractReindexingRule
{
  private final Granularity queryGranularity;
  private final Boolean rollup;

  @JsonCreator
  public ReindexingQueryGranularityRule(
      @JsonProperty("id") @Nonnull String id,
      @JsonProperty("description") @Nullable String description,
      @JsonProperty("olderThan") @Nonnull Period olderThan,
      @JsonProperty("queryGranularity") @Nonnull Granularity queryGranularity,
      @JsonProperty("rollup") @Nonnull Boolean rollup
  )
  {
    super(id, description, olderThan);
    this.queryGranularity = Objects.requireNonNull(queryGranularity);
    this.rollup = Objects.requireNonNull(rollup);
  }

  @JsonProperty
  public Granularity getQueryGranularity()
  {
    return queryGranularity;
  }

  @JsonProperty
  public Boolean getRollup()
  {
    return rollup;
  }
}
