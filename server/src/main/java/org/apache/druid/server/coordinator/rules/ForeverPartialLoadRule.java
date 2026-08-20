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

package org.apache.druid.server.coordinator.rules;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Forever variant of {@link PartialLoadRule}. Mirrors {@link ForeverLoadRule} (always load) and layers on
 * {@link PartialLoadMatcher} for selection.
 */
public class ForeverPartialLoadRule extends PartialLoadRule
{
  public static final String TYPE = "loadPartialForever";

  @JsonCreator
  public ForeverPartialLoadRule(
      @JsonProperty("tieredReplicants") Map<String, Integer> tieredReplicants,
      @JsonProperty("useDefaultTierForNull") @Nullable Boolean useDefaultTierForNull,
      @JsonProperty("matcher") PartialLoadMatcher matcher,
      @JsonProperty("onCannotMatch") @Nullable CannotMatchBehavior onCannotMatch
  )
  {
    super(tieredReplicants, useDefaultTierForNull, matcher, onCannotMatch);
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
  {
    return true;
  }
}
