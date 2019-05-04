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

package org.apache.druid.query.aggregation.unique;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.util.Collections;
import java.util.List;

public class DruidUniqueModule implements DruidModule
{
  static final String TYPE_NAME = "unique";
  static final String BITMAP_NAME = "ImmutableRoaringBitmap";


  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("UniqueModule")
            .registerSubtypes(new NamedType(UniqueAggregatorFactory.class, TYPE_NAME))
            .addSerializer(ImmutableRoaringBitmap.class, new RoaringBitmapSerializer()));
  }

  @Override
  public void configure(Binder binder)
  {
    if (ComplexMetrics.getSerdeForType(TYPE_NAME) == null) {
      ComplexMetrics.registerSerde(TYPE_NAME, new RoaringBitmapComplexMetricSerde());
    }
    if (ComplexMetrics.getSerdeForType(BITMAP_NAME) == null) {
      ComplexMetrics.registerSerde(BITMAP_NAME, new RoaringBitmapComplexMetricSerde());
    }
  }
}
