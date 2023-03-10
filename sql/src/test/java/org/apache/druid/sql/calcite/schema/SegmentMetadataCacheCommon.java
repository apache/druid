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

package org.apache.druid.sql.calcite.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.Joinable;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.sql.calcite.planner.SegmentMetadataCacheConfig;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.easymock.EasyMock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public abstract class SegmentMetadataCacheCommon extends CalciteTestBase
{
  static final SegmentMetadataCacheConfig SEGMENT_CACHE_CONFIG_DEFAULT = SegmentMetadataCacheConfig.create("PT1S");

  static final List<InputRow> ROWS1 = ImmutableList.of(
      TestDataBuilder.createRow(ImmutableMap.of("t", "2000-01-01", "m1", "1.0", "dim1", "")),
      TestDataBuilder.createRow(ImmutableMap.of("t", "2000-01-02", "m1", "2.0", "dim1", "10.1")),
      TestDataBuilder.createRow(ImmutableMap.of("t", "2000-01-03", "m1", "3.0", "dim1", "2"))
  );

  static final List<InputRow> ROWS2 = ImmutableList.of(
      TestDataBuilder.createRow(ImmutableMap.of("t", "2001-01-01", "m1", "4.0", "dim2", ImmutableList.of("a"))),
      TestDataBuilder.createRow(ImmutableMap.of("t", "2001-01-02", "m1", "5.0", "dim2", ImmutableList.of("abc"))),
      TestDataBuilder.createRow(ImmutableMap.of("t", "2001-01-03", "m1", "6.0"))
  );

  static QueryRunnerFactoryConglomerate conglomerate;
  static Closer resourceCloser;

  CountDownLatch getDatasourcesLatch = new CountDownLatch(1);

  @BeforeClass
  public static void setUpClass()
  {
    resourceCloser = Closer.create();
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(resourceCloser);
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    resourceCloser.close();
  }

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  SegmentManager segmentManager;
  Set<String> segmentDataSourceNames;
  Set<String> joinableDataSourceNames;
  JoinableFactory globalTableJoinable;

  @Before
  public void setUpCommon()
  {
    segmentDataSourceNames = Sets.newConcurrentHashSet();
    joinableDataSourceNames = Sets.newConcurrentHashSet();

    segmentManager = new SegmentManager(EasyMock.createMock(SegmentLoader.class))
    {
      @Override
      public Set<String> getDataSourceNames()
      {
        getDatasourcesLatch.countDown();
        return segmentDataSourceNames;
      }
    };

    globalTableJoinable = new JoinableFactory()
    {
      @Override
      public boolean isDirectlyJoinable(DataSource dataSource)
      {
        return dataSource instanceof GlobalTableDataSource &&
               joinableDataSourceNames.contains(((GlobalTableDataSource) dataSource).getName());
      }

      @Override
      public Optional<Joinable> build(
          DataSource dataSource,
          JoinConditionAnalysis condition
      )
      {
        return Optional.empty();
      }
    };
  }
}
