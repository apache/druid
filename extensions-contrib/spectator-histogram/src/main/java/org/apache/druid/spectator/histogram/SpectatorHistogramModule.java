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

package org.apache.druid.spectator.histogram;

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
 * Module defining various aggregators for Spectator Histograms
 */
public class SpectatorHistogramModule implements DruidModule
{
  @VisibleForTesting
  public static void registerSerde()
  {
    ComplexMetrics.registerSerde(
        SpectatorHistogramAggregatorFactory.TYPE_NAME,
        new SpectatorHistogramComplexMetricSerde(SpectatorHistogramAggregatorFactory.TYPE_NAME)
    );
    ComplexMetrics.registerSerde(
        SpectatorHistogramAggregatorFactory.Timer.TYPE_NAME,
        new SpectatorHistogramComplexMetricSerde(SpectatorHistogramAggregatorFactory.Timer.TYPE_NAME)
    );
    ComplexMetrics.registerSerde(
        SpectatorHistogramAggregatorFactory.Distribution.TYPE_NAME,
        new SpectatorHistogramComplexMetricSerde(SpectatorHistogramAggregatorFactory.Distribution.TYPE_NAME)
    );
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule(
            getClass().getSimpleName()
        ).registerSubtypes(
            new NamedType(
                SpectatorHistogramAggregatorFactory.class,
                SpectatorHistogramAggregatorFactory.TYPE_NAME
            ),
            new NamedType(
                SpectatorHistogramAggregatorFactory.Timer.class,
                SpectatorHistogramAggregatorFactory.Timer.TYPE_NAME
            ),
            new NamedType(
                SpectatorHistogramAggregatorFactory.Distribution.class,
                SpectatorHistogramAggregatorFactory.Distribution.TYPE_NAME
            ),
            new NamedType(
                SpectatorHistogramPercentilePostAggregator.class,
                SpectatorHistogramPercentilePostAggregator.TYPE_NAME
            ),
            new NamedType(
                SpectatorHistogramPercentilesPostAggregator.class,
                SpectatorHistogramPercentilesPostAggregator.TYPE_NAME
            )
        ).addSerializer(SpectatorHistogram.class, new SpectatorHistogramJsonSerializer())
    );
  }

  @Override
  public void configure(Binder binder)
  {
    registerSerde();
  }
}
