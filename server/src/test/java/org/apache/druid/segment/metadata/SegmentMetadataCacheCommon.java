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

package org.apache.druid.segment.metadata;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class SegmentMetadataCacheCommon
{
  public static final String DATASOURCE1 = "foo";
  public static final String DATASOURCE2 = "foo2";
  public static final String DATASOURCE3 = "numfoo";
  public static final String DATASOURCE4 = "foo4";
  public static final String DATASOURCE5 = "lotsocolumns";
  public static final String BROADCAST_DATASOURCE = "broadcast";
  public static final String FORBIDDEN_DATASOURCE = "forbiddenDatasource";
  public static final String SOME_DATASOURCE = "some_datasource";
  public static final String TIMESTAMP_COLUMN = "t";
  private static final InputRowSchema FOO_SCHEMA = new InputRowSchema(
      new TimestampSpec(TIMESTAMP_COLUMN, "iso", null),
      new DimensionsSpec(
          DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2", "dim3"))
      ),
      null
  );

  static final SegmentMetadataCacheConfig SEGMENT_CACHE_CONFIG_DEFAULT = SegmentMetadataCacheConfig.create("PT1S");

  final List<InputRow> ROWS1 = ImmutableList.of(
      createRow(ImmutableMap.of("t", "2000-01-01", "m1", "1.0", "dim1", "")),
      createRow(ImmutableMap.of("t", "2000-01-02", "m1", "2.0", "dim1", "10.1")),
      createRow(ImmutableMap.of("t", "2000-01-03", "m1", "3.0", "dim1", "2"))
  );

  final List<InputRow> ROWS2 = ImmutableList.of(
      createRow(ImmutableMap.of("t", "2001-01-01", "m1", "4.0", "dim2", ImmutableList.of("a"))),
      createRow(ImmutableMap.of("t", "2001-01-02", "m1", "5.0", "dim2", ImmutableList.of("abc"))),
      createRow(ImmutableMap.of("t", "2001-01-03", "m1", "6.0"))
  );

  static QueryRunnerFactoryConglomerate conglomerate;
  static Closer resourceCloser;
  static QueryToolChestWarehouse queryToolChestWarehouse;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();


  @BeforeClass
  public static void setUpClass()
  {
    resourceCloser = Closer.create();
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(resourceCloser);
    queryToolChestWarehouse = new QueryToolChestWarehouse()
    {
      @Override
      public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(final QueryType query)
      {
        return conglomerate.findFactory(query).getToolchest();
      }
    };
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    resourceCloser.close();
  }

  InputRow createRow(final ImmutableMap<String, ?> map)
  {
    return MapInputRowParser.parse(FOO_SCHEMA, (Map<String, Object>) map);
  }

  InputRow createRow(final ImmutableMap<String, ?> map, InputRowSchema inputRowSchema)
  {
    return MapInputRowParser.parse(inputRowSchema, (Map<String, Object>) map);
  }

  QueryLifecycleFactory getQueryLifecycleFactory(QuerySegmentWalker walker)
  {
    return new QueryLifecycleFactory(
        queryToolChestWarehouse,
        walker,
        new DefaultGenericQueryMetricsFactory(),
        new NoopServiceEmitter(),
        new TestRequestLogger(),
        new AuthConfig(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of()))
    );
  }
}
