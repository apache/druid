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

package org.apache.druid.query.aggregation.ddsketch;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.segment.serde.ComplexMetrics;

import java.util.List;

/**
 * Module defining aggregators for the DDsketch based sketches
 */
public class DDSketchModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule(
            getClass().getSimpleName()
        ).registerSubtypes(
            new NamedType(
                DDSketchAggregatorFactory.class,
                DDSketchAggregatorFactory.TYPE_NAME
            ),
            new NamedType(
                DDSketchToQuantilesPostAggregator.class,
                DDSketchToQuantilesPostAggregator.TYPE_NAME
            ),
            new NamedType(
                DDSketchToQuantilePostAggregator.class,
                DDSketchToQuantilePostAggregator.TYPE_NAME
            )
        ).addSerializer(DDSketch.class, new DDSketchJsonSerializer())
    );
  }

  @Override
  public void configure(Binder binder)
  {
    registerSerde();
  }

  @VisibleForTesting
  public static void registerSerde()
  {
    ComplexMetrics.registerSerde(DDSketchAggregatorFactory.TYPE_NAME, new DDSketchComplexMetricSerde());
  }

}
