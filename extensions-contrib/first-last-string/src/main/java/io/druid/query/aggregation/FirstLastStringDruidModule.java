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

package io.druid.query.aggregation;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;
import io.druid.query.aggregation.first.StringFirstAggregatorFactory;
import io.druid.query.aggregation.last.StringLastAggregatorFactory;
import io.druid.segment.serde.ComplexMetrics;

import java.util.List;

/**
 */
public class FirstLastStringDruidModule implements DruidModule
{
  public static final String STRING_LAST = "stringLast";
  public static final String STRING_FIRST = "stringFirst";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("FirstLastStringModule").registerSubtypes(
            new NamedType(StringLastAggregatorFactory.class, STRING_LAST),
            new NamedType(StringFirstAggregatorFactory.class, STRING_FIRST)
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    if (ComplexMetrics.getSerdeForType("serializablePairLongString") == null) {
      ComplexMetrics.registerSerde("serializablePairLongString", new SerializablePairSerde());
    }
  }
}
