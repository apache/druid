/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.io.IOException;
import java.util.Set;

public class IndexerSQLMetadataStorageCoordinatorTest
{

  private final MetadataStorageTablesConfig tablesConfig = MetadataStorageTablesConfig.fromBase("test");
  private final TestDerbyConnector derbyConnector = new TestDerbyConnector(
      Suppliers.ofInstance(new MetadataStorageConnectorConfig()),
      Suppliers.ofInstance(tablesConfig)
  );
  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final DataSegment defaultSegment = new DataSegment(
      "dataSource",
      Interval.parse("2015-01-01T00Z/2015-01-02T00Z"),
      "version",
      ImmutableMap.<String, Object>of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new LinearShardSpec(0),
      9,
      100
  );

  private final DataSegment defaultSegment2 = new DataSegment(
      "dataSource",
      Interval.parse("2015-01-01T00Z/2015-01-02T00Z"),
      "version",
      ImmutableMap.<String, Object>of(),
      ImmutableList.of("dim1"),
      ImmutableList.of("m1"),
      new LinearShardSpec(1),
      9,
      100
  );
  private final Set<DataSegment> segments = ImmutableSet.of(defaultSegment, defaultSegment2);
  IndexerSQLMetadataStorageCoordinator coordinator;

  @Before
  public void setUp()
  {
    mapper.registerSubtypes(LinearShardSpec.class);
    derbyConnector.createTaskTables();
    derbyConnector.createSegmentTable();
    coordinator = new IndexerSQLMetadataStorageCoordinator(
        mapper,
        tablesConfig,
        derbyConnector
    );
  }

  @After
  public void tearDown()
  {
    derbyConnector.tearDown();
  }

  private void unUseSegment()
  {
    for (final DataSegment segment : segments) {
      Assert.assertEquals(
          1, (int) derbyConnector.getDBI().<Integer>withHandle(
              new HandleCallback<Integer>()
              {
                @Override
                public Integer withHandle(Handle handle) throws Exception
                {
                  return handle.createStatement(
                      String.format("UPDATE %s SET used = false WHERE id = :id", tablesConfig.getSegmentsTable())
                  )
                               .bind("id", segment.getIdentifier())
                               .execute();
                }
              }
          )
      );
    }
  }

  @Test
  public void testSimpleAnnounce() throws IOException
  {
    coordinator.announceHistoricalSegments(segments);
    Assert.assertArrayEquals(
        mapper.writeValueAsString(defaultSegment).getBytes("UTF-8"),
        derbyConnector.lookup(
            tablesConfig.getSegmentsTable(),
            "id",
            "payload",
            defaultSegment.getIdentifier()
        )
    );
  }

  @Test
  public void testSimpleUsedList() throws IOException
  {
    coordinator.announceHistoricalSegments(segments);
    Assert.assertEquals(
        segments,
        ImmutableSet.copyOf(
            coordinator.getUsedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval()
            )
        )
    );
  }

  @Test
  public void testSimpleUnUsedList() throws IOException
  {
    coordinator.announceHistoricalSegments(segments);
    unUseSegment();
    Assert.assertEquals(
        segments,
        ImmutableSet.copyOf(
            coordinator.getUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval()
            )
        )
    );
  }


  @Test
  public void testUsedOverlapLow() throws IOException
  {
    coordinator.announceHistoricalSegments(segments);
    Set<DataSegment> actualSegments = ImmutableSet.copyOf(
        coordinator.getUsedSegmentsForInterval(
            defaultSegment.getDataSource(),
            Interval.parse("2014-12-31T23:59:59.999Z/2015-01-01T00:00:00.001Z") // end is exclusive
        )
    );
    Assert.assertEquals(
        segments,
        actualSegments
    );
  }


  @Test
  public void testUsedOverlapHigh() throws IOException
  {
    coordinator.announceHistoricalSegments(segments);
    Assert.assertEquals(
        segments,
        ImmutableSet.copyOf(
            coordinator.getUsedSegmentsForInterval(
                defaultSegment.getDataSource(),
                Interval.parse("2015-1-1T23:59:59.999Z/2015-02-01T00Z")
            )
        )
    );
  }

  @Test
  public void testUsedOutOfBoundsLow() throws IOException
  {
    coordinator.announceHistoricalSegments(segments);
    Assert.assertTrue(
        coordinator.getUsedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(defaultSegment.getInterval().getStart().minus(1), defaultSegment.getInterval().getStart())
        ).isEmpty()
    );
  }


  @Test
  public void testUsedOutOfBoundsHigh() throws IOException
  {
    coordinator.announceHistoricalSegments(segments);
    Assert.assertTrue(
        coordinator.getUsedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(defaultSegment.getInterval().getEnd(), defaultSegment.getInterval().getEnd().plusDays(10))
        ).isEmpty()
    );
  }

  @Test
  public void testUsedWithinBoundsEnd() throws IOException
  {
    coordinator.announceHistoricalSegments(segments);
    Assert.assertEquals(
        segments,
        ImmutableSet.copyOf(
            coordinator.getUsedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withEnd(defaultSegment.getInterval().getEnd().minusMillis(1))
            )
        )
    );
  }

  @Test
  public void testUsedOverlapEnd() throws IOException
  {
    coordinator.announceHistoricalSegments(segments);
    Assert.assertEquals(
        segments,
        ImmutableSet.copyOf(
            coordinator.getUsedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withEnd(defaultSegment.getInterval().getEnd().plusMillis(1))
            )
        )
    );
  }


  @Test
  public void testUnUsedOverlapLow() throws IOException
  {
    coordinator.announceHistoricalSegments(segments);
    unUseSegment();
    Assert.assertTrue(
        coordinator.getUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(
                defaultSegment.getInterval().getStart().minus(1),
                defaultSegment.getInterval().getStart().plus(1)
            )
        ).isEmpty()
    );
  }

  @Test
  public void testUnUsedUnderlapLow() throws IOException
  {
    coordinator.announceHistoricalSegments(segments);
    unUseSegment();
    Assert.assertTrue(
        coordinator.getUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(defaultSegment.getInterval().getStart().plus(1), defaultSegment.getInterval().getEnd())
        ).isEmpty()
    );
  }


  @Test
  public void testUnUsedUnderlapHigh() throws IOException
  {
    coordinator.announceHistoricalSegments(segments);
    unUseSegment();
    Assert.assertTrue(
        coordinator.getUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            new Interval(defaultSegment.getInterval().getStart(), defaultSegment.getInterval().getEnd().minus(1))
        ).isEmpty()
    );
  }

  @Test
  public void testUnUsedOverlapHigh() throws IOException
  {
    coordinator.announceHistoricalSegments(segments);
    unUseSegment();
    Assert.assertTrue(
        coordinator.getUnusedSegmentsForInterval(
            defaultSegment.getDataSource(),
            defaultSegment.getInterval().withStart(defaultSegment.getInterval().getEnd().minus(1))
        ).isEmpty()
    );
  }

  @Test
  public void testUnUsedBigOverlap() throws IOException
  {
    coordinator.announceHistoricalSegments(segments);
    unUseSegment();
    Assert.assertEquals(
        segments,
        ImmutableSet.copyOf(
            coordinator.getUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                Interval.parse("2000/2999")
            )
        )
    );
  }

  @Test
  public void testUnUsedLowRange() throws IOException
  {
    coordinator.announceHistoricalSegments(segments);
    unUseSegment();
    Assert.assertEquals(
        segments,
        ImmutableSet.copyOf(
            coordinator.getUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withStart(defaultSegment.getInterval().getStart().minus(1))
            )
        )
    );
    Assert.assertEquals(
        segments,
        ImmutableSet.copyOf(
            coordinator.getUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withStart(defaultSegment.getInterval().getStart().minusYears(1))
            )
        )
    );
  }

  @Test
  public void testUnUsedHighRange() throws IOException
  {
    coordinator.announceHistoricalSegments(segments);
    unUseSegment();
    Assert.assertEquals(
        segments,
        ImmutableSet.copyOf(
            coordinator.getUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withEnd(defaultSegment.getInterval().getEnd().plus(1))
            )
        )
    );
    Assert.assertEquals(
        segments,
        ImmutableSet.copyOf(
            coordinator.getUnusedSegmentsForInterval(
                defaultSegment.getDataSource(),
                defaultSegment.getInterval().withEnd(defaultSegment.getInterval().getEnd().plusYears(1))
            )
        )
    );
  }
}
