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
import org.apache.druid.server.SegmentManager;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.Set;

public class PhysicalDataSourceMetadataFactoryTest
{
  private Set<String> segmentDataSourceNames;
  private Set<String> joinableDataSourceNames;
  private SegmentManager segmentManager;
  private JoinableFactory globalTableJoinable;

  private PhysicalDatasourceMetadataFactory datasourceMetadataFactory;

  @Before
  public void setUp()
  {
    segmentDataSourceNames = Sets.newConcurrentHashSet();
    joinableDataSourceNames = Sets.newConcurrentHashSet();
    segmentManager = new SegmentManager(EasyMock.createMock(SegmentLoader.class))
    {
      @Override
      public Set<String> getDataSourceNames()
      {
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

    datasourceMetadataFactory = new PhysicalDatasourceMetadataFactory(globalTableJoinable, segmentManager);
  }

  @Test
  public void testBuild()
  {
    segmentDataSourceNames.add("foo");
    joinableDataSourceNames.add("foo");

    RowSignature fooSignature =
        RowSignature.builder()
                    .add("c1", ColumnType.FLOAT)
                    .add("c2", ColumnType.DOUBLE)
                    .build();

    RowSignature barSignature =
        RowSignature.builder()
                    .add("d1", ColumnType.FLOAT)
                    .add("d2", ColumnType.DOUBLE)
                    .build();

    DatasourceTable.PhysicalDatasourceMetadata fooDs = datasourceMetadataFactory.build("foo", fooSignature);
    Assert.assertTrue(fooDs.isJoinable());
    Assert.assertTrue(fooDs.isBroadcast());
    Assert.assertEquals(fooDs.dataSource().getName(), "foo");
    Assert.assertEquals(fooDs.getRowSignature(), fooSignature);

    DatasourceTable.PhysicalDatasourceMetadata barDs = datasourceMetadataFactory.build("bar", barSignature);
    Assert.assertFalse(barDs.isJoinable());
    Assert.assertFalse(barDs.isBroadcast());
    Assert.assertEquals(barDs.dataSource().getName(), "bar");
    Assert.assertEquals(barDs.getRowSignature(), barSignature);
  }
}
