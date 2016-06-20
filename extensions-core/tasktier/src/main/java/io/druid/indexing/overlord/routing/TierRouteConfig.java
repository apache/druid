/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord.routing;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class TierRouteConfig
{
  @VisibleForTesting
  @JsonProperty("tierMap")
  Map<String, Map<String, Object>> tierMap = ImmutableMap.of();

  @JacksonInject
  ObjectMapper mapper = null;

  public
  TierTaskRunnerFactory getRouteFactory(String tier)
  {
    final Map<String, Object> map = tierMap.get(Preconditions.checkNotNull(tier, "tier"));
    if (map == null) {
      throw new NullPointerException(
          String.format(
              "No tier found for [%s]. Valid tier are %s", tier,
              Arrays.toString(tierMap.keySet().toArray())
          )
      );
    }
    return mapper.convertValue(map, TierTaskRunnerFactory.class);
  }

  public Set<String> getTiers()
  {
    return tierMap.keySet();
  }
}
