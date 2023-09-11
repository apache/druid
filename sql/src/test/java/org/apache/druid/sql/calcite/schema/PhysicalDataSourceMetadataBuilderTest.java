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

import com.google.common.collect.Sets;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.Joinable;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.metadata.DataSourceInformation;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class PhysicalDataSourceMetadataBuilderTest
{
  private CountDownLatch getDatasourcesLatch = new CountDownLatch(1);

  private Set<String> segmentDataSourceNames;
  private Set<String> joinableDataSourceNames;
  private SegmentManager segmentManager;
  private JoinableFactory globalTableJoinable;

  private PhysicalDatasourceMetadataBuilder physicalDatasourceMetadataBuilder;

  @Before
  public void setUp() throws Exception
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

    physicalDatasourceMetadataBuilder = new PhysicalDatasourceMetadataBuilder(globalTableJoinable, segmentManager);
  }

  @Test
  public void testBuild()
  {
    segmentDataSourceNames.add("foo");
    joinableDataSourceNames.add("foo");

    DataSourceInformation foo =
        new DataSourceInformation(
            "foo",
            RowSignature.builder()
                        .add("c1", ColumnType.FLOAT)
                        .add("c2", ColumnType.DOUBLE)
                        .build()
        );

    DataSourceInformation bar =
        new DataSourceInformation(
            "bar",
            RowSignature.builder()
                        .add("d1", ColumnType.FLOAT)
                        .add("d2", ColumnType.DOUBLE)
                        .build()
        );

    DatasourceTable.PhysicalDatasourceMetadata fooDs = physicalDatasourceMetadataBuilder.build(foo);
    Assert.assertTrue(fooDs.isJoinable());
    Assert.assertTrue(fooDs.isBroadcast());
    Assert.assertEquals(fooDs.dataSource().getName(), foo.getDatasource());
    Assert.assertEquals(fooDs.rowSignature(), foo.getRowSignature());

    DatasourceTable.PhysicalDatasourceMetadata barDs = physicalDatasourceMetadataBuilder.build(bar);
    Assert.assertFalse(barDs.isJoinable());
    Assert.assertFalse(barDs.isBroadcast());
    Assert.assertEquals(barDs.dataSource().getName(), bar.getDatasource());
    Assert.assertEquals(barDs.rowSignature(), bar.getRowSignature());
  }
}
