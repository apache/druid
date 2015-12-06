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

package io.druid.jackson;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.druid.granularity.AllGranularity;
import io.druid.granularity.DurationGranularity;
import io.druid.granularity.NoneGranularity;
import io.druid.granularity.PeriodGranularity;
import io.druid.granularity.QueryGranularity;

/**
 */
public class QueryGranularityModule extends SimpleModule
{
  public QueryGranularityModule()
  {
    super("QueryGranularityModule");

    setMixInAnnotation(QueryGranularity.class, QueryGranularityMixin.class);
    registerSubtypes(
        new NamedType(PeriodGranularity.class, "period"),
        new NamedType(DurationGranularity.class, "duration"),
        new NamedType(AllGranularity.class, "all"),
        new NamedType(NoneGranularity.class, "none")
    );
  }

  @JsonTypeInfo(use= JsonTypeInfo.Id.NAME, property = "type", defaultImpl = QueryGranularity.class)
  public static interface QueryGranularityMixin {}
}
