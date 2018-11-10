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

package org.apache.druid.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.metadata.SegmentMetadataQueryConfig;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Properties;

public class SegmentMetadataQueryConfigTest
{
  @Test
  public void testSerdeSegmentMetadataQueryConfig()
  {
    Injector injector = Guice.createInjector(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.install(new PropertiesModule(Collections.singletonList("test.runtime.properties")));
            binder.install(new ConfigModule());
            binder.install(new DruidGuiceExtensions());
            JsonConfigProvider.bind(binder, "druid.query.segmentMetadata", SegmentMetadataQueryConfig.class);
          }

          @Provides
          @LazySingleton
          public ObjectMapper jsonMapper()
          {
            return new DefaultObjectMapper();
          }
        }
    );


    Properties props = injector.getInstance(Properties.class);
    SegmentMetadataQueryConfig config = injector.getInstance(SegmentMetadataQueryConfig.class);

    EnumSet<SegmentMetadataQuery.AnalysisType> expectedDefaultAnalysis = config.getDefaultAnalysisTypes();
    String actualDefaultAnalysis = props.getProperty("druid.query.segmentMetadata.defaultAnalysisTypes");

    Iterator<SegmentMetadataQuery.AnalysisType> it = expectedDefaultAnalysis.iterator();
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    while (it.hasNext()) {
      SegmentMetadataQuery.AnalysisType e = it.next();
      sb.append("\"" + e + "\"");
      if (it.hasNext()) {
        sb.append(',').append(' ');
      }
    }
    sb.append(']');

    String expectedDefaultAnalysisAsString = sb.toString();

    Assert.assertEquals(
        expectedDefaultAnalysisAsString,
        actualDefaultAnalysis
    );
    Assert.assertEquals(
        props.getProperty("druid.query.segmentMetadata.defaultHistory"),
        config.getDefaultHistory().toString()
    );
  }
}
