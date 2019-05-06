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

package org.apache.druid.query.aggregation.tdigestsketch;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.tdunning.math.stats.MergingDigest;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.segment.serde.ComplexMetrics;

import java.util.List;

/**
 * Module defining aggregators for the T-Digest based sketches
 */
public class TDigestSketchModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule(
            getClass().getSimpleName()
        ).registerSubtypes(
            new NamedType(
                TDigestBuildSketchAggregatorFactory.class,
                TDigestBuildSketchAggregatorFactory.TYPE_NAME
            ),
            new NamedType(
                TDigestMergeSketchAggregatorFactory.class,
                TDigestMergeSketchAggregatorFactory.TYPE_NAME
            ),
            new NamedType(
                TDigestSketchToQuantilesPostAggregator.class,
                TDigestSketchToQuantilesPostAggregator.TYPE_NAME
            )
        ).addSerializer(MergingDigest.class, new TDigestSketchJsonSerializer())
    );
  }

  @Override
  public void configure(Binder binder)
  {
    registerSerde();
  }

  @VisibleForTesting
  static void registerSerde()
  {
    if (ComplexMetrics.getSerdeForType(TDigestBuildSketchAggregatorFactory.TYPE_NAME) == null) {
      ComplexMetrics.registerSerde(TDigestBuildSketchAggregatorFactory.TYPE_NAME, new TDigestSketchComplexMetricSerde());
    }
  }

}
