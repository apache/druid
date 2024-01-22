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
import com.google.common.collect.Sets;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.Joinable;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.metadata.SegmentMetadataCacheCommon;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.util.TestTimelineServerView;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class BrokerSegmentMetadataCacheCommon extends SegmentMetadataCacheCommon
{
  public List<ImmutableDruidServer> druidServers;
  SegmentManager segmentManager;
  Set<String> segmentDataSourceNames;
  Set<String> joinableDataSourceNames;
  JoinableFactory globalTableJoinable;

  public SpecificSegmentsQuerySegmentWalker walker;
  public TestTimelineServerView serverView;

  public void setUp() throws Exception
  {
    setUpData();
    setUpCommon();

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

    walker = SpecificSegmentsQuerySegmentWalker.createWalker(conglomerate)
        .add(segment1, index1)
        .add(segment2, index2)
        .add(segment3, index2)
        .add(segment4, indexAuto1)
        .add(segment5, indexAuto2);

    final List<DataSegment> realtimeSegments = ImmutableList.of(realtimeSegment1);
    serverView = new TestTimelineServerView(walker.getSegments(), realtimeSegments);
    druidServers = serverView.getDruidServers();
  }
}
