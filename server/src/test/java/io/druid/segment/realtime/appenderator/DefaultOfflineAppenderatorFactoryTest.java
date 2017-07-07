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

package io.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.guice.GuiceInjectors;
import io.druid.initialization.Initialization;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.DruidProcessingConfig;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.timeline.partition.LinearShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Map;

public class DefaultOfflineAppenderatorFactoryTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testBuild() throws IOException, SegmentNotWritableException
  {
    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(new Module()
                                 {
                                   @Override
                                   public void configure(Binder binder)
                                   {
                                     binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/tool");
                                     binder.bindConstant().annotatedWith(Names.named("servicePort")).to(9999);
                                     binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
                                     binder.bind(DruidProcessingConfig.class).toInstance(
                                         new DruidProcessingConfig()
                                         {
                                           @Override
                                           public String getFormatString()
                                           {
                                             return "processing-%s";
                                           }

                                           @Override
                                           public int intermediateComputeSizeBytes()
                                           {
                                             return 100 * 1024 * 1024;
                                           }

                                           @Override
                                           public int getNumThreads()
                                           {
                                             return 1;
                                           }

                                           @Override
                                           public int columnCacheSizeBytes()
                                           {
                                             return 25 * 1024 * 1024;
                                           }
                                         }
                                     );
                                     binder.bind(ColumnConfig.class).to(DruidProcessingConfig.class);
                                   }
                                 }
        )
    );
    ObjectMapper objectMapper = injector.getInstance(ObjectMapper.class);
    AppenderatorFactory defaultOfflineAppenderatorFactory = objectMapper.reader(AppenderatorFactory.class)
                                                                        .readValue("{\"type\":\"offline\"}");

    final Map<String, Object> parserMap = objectMapper.convertValue(
        new MapInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("ts", "auto", null),
                new DimensionsSpec(null, null, null),
                null,
                null
            )
        ),
        Map.class
    );
    DataSchema schema = new DataSchema(
        "dataSourceName",
        parserMap,
        new AggregatorFactory[]{
            new CountAggregatorFactory("count"),
            new LongSumAggregatorFactory("met", "met")
        },
        new UniformGranularitySpec(Granularities.MINUTE, Granularities.NONE, null),
        objectMapper
    );

    RealtimeTuningConfig tuningConfig = new RealtimeTuningConfig(
        75000,
        null,
        null,
        temporaryFolder.newFolder(),
        null,
        null,
        null,
        null,
        null,
        null,
        0,
        0,
        null,
        null,
        null
    );

    try (Appenderator appenderator = defaultOfflineAppenderatorFactory.build(
        schema,
        tuningConfig,
        new FireDepartmentMetrics()
    )) {
      Assert.assertEquals("dataSourceName", appenderator.getDataSource());
      Assert.assertEquals(null, appenderator.startJob());
      SegmentIdentifier identifier = new SegmentIdentifier(
          "dataSourceName",
          new Interval("2000/2001"),
          "A",
          new LinearShardSpec(0)
      );
      Assert.assertEquals(0, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.add(identifier, AppenderatorTest.IR("2000", "bar", 1), Suppliers.ofInstance(Committers.nil()));
      Assert.assertEquals(1, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.add(identifier, AppenderatorTest.IR("2000", "baz", 1), Suppliers.ofInstance(Committers.nil()));
      Assert.assertEquals(2, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.close();
      Assert.assertEquals(0, ((AppenderatorImpl) appenderator).getRowsInMemory());
    }
  }
}
