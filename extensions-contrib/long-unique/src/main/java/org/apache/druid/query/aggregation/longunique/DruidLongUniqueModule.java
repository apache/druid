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

package org.apache.druid.query.aggregation.longunique;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.Collections;
import java.util.List;

public class DruidLongUniqueModule implements DruidModule
{
  static final String TYPE_NAME = "longUnique";
  static final String BITMAP_NAME = "Roaring64NavigableMap";


  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("LongUniqueModule")
            .registerSubtypes(new NamedType(LongUniqueAggregatorFactory.class, TYPE_NAME))
            .addSerializer(Roaring64NavigableMap.class, new LongRoaringBitmapSerializer()));
  }

  @Override
  public void configure(Binder binder)
  {
    if (ComplexMetrics.getSerdeForType(TYPE_NAME) == null) {
      ComplexMetrics.registerSerde(TYPE_NAME, new LongRoaringBitmapComplexMetricSerde());
    }
    if (ComplexMetrics.getSerdeForType(BITMAP_NAME) == null) {
      ComplexMetrics.registerSerde(BITMAP_NAME, new LongRoaringBitmapComplexMetricSerde());
    }
  }
}
