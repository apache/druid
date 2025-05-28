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

package org.apache.druid.indexing.seekablestream.common;

import org.apache.druid.data.input.impl.ByteEntity;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class RecordSupplierGroupTest
{

  private RecordSupplier<TestPartitionId, Long, ByteEntity> mockSupplier1;
  private RecordSupplier<TestPartitionId, Long, ByteEntity> mockSupplier2;
  private RecordSupplierGroup<TestPartitionId, Long, ByteEntity> recordSupplierGroup;
  private List<String> keys;
  private List<RecordSupplier<TestPartitionId, Long, ByteEntity>> suppliers;

  private static class TestPartitionId implements PartitionId
  {
    private final String cluster;

    public TestPartitionId(final String cluster)
    {
      this.cluster = cluster;
    }

    @Override
    public String getCluster()
    {
      return cluster;
    }
  }

  @Before
  public void setUp()
  {
    mockSupplier1 = EasyMock.createMock(RecordSupplier.class);
    mockSupplier2 = EasyMock.createMock(RecordSupplier.class);
    keys = Arrays.asList("cluster1", "cluster2");
    suppliers = Arrays.asList(mockSupplier1, mockSupplier2);
    recordSupplierGroup = new RecordSupplierGroup<>(keys, suppliers);
  }

  @Test
  public void testAssign()
  {
    Set<StreamPartition<TestPartitionId>> partitions1 = new HashSet<>();
    Set<StreamPartition<TestPartitionId>> partitions2 = new HashSet<>();

    partitions1.add(new StreamPartition<>("stream-1", new TestPartitionId("cluster1")));
    partitions2.add(new StreamPartition<>("stream-2", new TestPartitionId("cluster2")));

    mockSupplier1.assign(partitions1);
    mockSupplier2.assign(partitions2);

    EasyMock.replay(mockSupplier1, mockSupplier2);

    recordSupplierGroup.assign(partitions1);
    recordSupplierGroup.assign(partitions2);

    EasyMock.verify(mockSupplier1, mockSupplier2);
  }

  @Test
  public void testSeek() throws InterruptedException
  {
    StreamPartition<TestPartitionId> partition1 = new StreamPartition<>("stream-1", new TestPartitionId("cluster1"));
    StreamPartition<TestPartitionId> partition2 = new StreamPartition<>("stream-2", new TestPartitionId("cluster2"));

    mockSupplier1.seek(partition1, 100L);
    mockSupplier2.seek(partition2, 200L);

    EasyMock.replay(mockSupplier1, mockSupplier2);

    recordSupplierGroup.seek(partition1, 100L);
    recordSupplierGroup.seek(partition2, 200L);

    EasyMock.verify(mockSupplier1, mockSupplier2);
  }

  @Test
  public void testSeekToEarliest() throws InterruptedException
  {
    Set<StreamPartition<TestPartitionId>> partitions1 = new HashSet<>();
    Set<StreamPartition<TestPartitionId>> partitions2 = new HashSet<>();

    partitions1.add(new StreamPartition<>("stream-1", new TestPartitionId("cluster1")));
    partitions2.add(new StreamPartition<>("stream-2", new TestPartitionId("cluster2")));

    mockSupplier1.seekToEarliest(partitions1);
    mockSupplier2.seekToEarliest(partitions2);

    EasyMock.replay(mockSupplier1, mockSupplier2);

    recordSupplierGroup.seekToEarliest(partitions1);
    recordSupplierGroup.seekToEarliest(partitions2);

    EasyMock.verify(mockSupplier1, mockSupplier2);
  }

  @Test
  public void testSeekToLatest() throws InterruptedException
  {
    Set<StreamPartition<TestPartitionId>> partitions1 = new HashSet<>();
    Set<StreamPartition<TestPartitionId>> partitions2 = new HashSet<>();

    partitions1.add(new StreamPartition<>("stream-1", new TestPartitionId("cluster1")));
    partitions2.add(new StreamPartition<>("stream-2", new TestPartitionId("cluster2")));

    mockSupplier1.seekToLatest(partitions1);
    mockSupplier2.seekToLatest(partitions2);

    EasyMock.replay(mockSupplier1, mockSupplier2);

    recordSupplierGroup.seekToLatest(partitions1);
    recordSupplierGroup.seekToLatest(partitions2);

    EasyMock.verify(mockSupplier1, mockSupplier2);
  }

  @Test
  public void testGetAssignment()
  {
    Collection<StreamPartition<TestPartitionId>> expectedAssignments1 = List.of(
        new StreamPartition<>("stream-1", new TestPartitionId("cluster1")));
    Collection<StreamPartition<TestPartitionId>> expectedAssignments2 = List.of(
        new StreamPartition<>("stream-2", new TestPartitionId("cluster2")));

    EasyMock.expect(mockSupplier1.getAssignment()).andReturn(expectedAssignments1);
    EasyMock.expect(mockSupplier2.getAssignment()).andReturn(expectedAssignments2);

    EasyMock.replay(mockSupplier1, mockSupplier2);

    Collection<StreamPartition<TestPartitionId>> assignments = recordSupplierGroup.getAssignment();

    assertEquals(2, assignments.size());

    EasyMock.verify(mockSupplier1, mockSupplier2);
  }

  @Test
  public void testPoll()
  {
    List<OrderedPartitionableRecord<TestPartitionId, Long, ByteEntity>> records1 = Collections.emptyList();
    List<OrderedPartitionableRecord<TestPartitionId, Long, ByteEntity>> records2 = Collections.emptyList();

    EasyMock.expect(mockSupplier1.poll(1)).andReturn(records1);
    EasyMock.expect(mockSupplier2.poll(1)).andReturn(records2);

    EasyMock.replay(mockSupplier1, mockSupplier2);

    List<OrderedPartitionableRecord<TestPartitionId, Long, ByteEntity>> records = recordSupplierGroup.poll(1);

    assertEquals(0, records.size());

    EasyMock.verify(mockSupplier1, mockSupplier2);
  }

  @Test
  public void testGetLatestSequenceNumber()
  {
    StreamPartition<TestPartitionId> partition1 = new StreamPartition<>("stream-1", new TestPartitionId("cluster1"));
    StreamPartition<TestPartitionId> partition2 = new StreamPartition<>("stream-2", new TestPartitionId("cluster2"));

    EasyMock.expect(mockSupplier1.getLatestSequenceNumber(partition1)).andReturn(100L);
    EasyMock.expect(mockSupplier2.getLatestSequenceNumber(partition2)).andReturn(200L);

    EasyMock.replay(mockSupplier1, mockSupplier2);

    assertEquals(Long.valueOf(100), recordSupplierGroup.getLatestSequenceNumber(partition1));
    assertEquals(Long.valueOf(200), recordSupplierGroup.getLatestSequenceNumber(partition2));

    EasyMock.verify(mockSupplier1, mockSupplier2);
  }

  @Test
  public void testGetEarliestSequenceNumber()
  {
    StreamPartition<TestPartitionId> partition1 = new StreamPartition<>("stream-1", new TestPartitionId("cluster1"));
    StreamPartition<TestPartitionId> partition2 = new StreamPartition<>("stream-2", new TestPartitionId("cluster2"));

    EasyMock.expect(mockSupplier1.getEarliestSequenceNumber(partition1)).andReturn(10L);
    EasyMock.expect(mockSupplier2.getEarliestSequenceNumber(partition2)).andReturn(20L);

    EasyMock.replay(mockSupplier1, mockSupplier2);

    assertEquals(Long.valueOf(10), recordSupplierGroup.getEarliestSequenceNumber(partition1));
    assertEquals(Long.valueOf(20), recordSupplierGroup.getEarliestSequenceNumber(partition2));

    EasyMock.verify(mockSupplier1, mockSupplier2);
  }

  @Test
  public void testIsOffsetAvailable()
  {
    StreamPartition<TestPartitionId> partition1 = new StreamPartition<>("stream-1", new TestPartitionId("cluster1"));
    StreamPartition<TestPartitionId> partition2 = new StreamPartition<>("stream-2", new TestPartitionId("cluster2"));

    OrderedSequenceNumber<Long> offset1 = EasyMock.createMock(OrderedSequenceNumber.class);
    OrderedSequenceNumber<Long> offset2 = EasyMock.createMock(OrderedSequenceNumber.class);

    EasyMock.expect(mockSupplier1.isOffsetAvailable(partition1, offset1)).andReturn(true);
    EasyMock.expect(mockSupplier2.isOffsetAvailable(partition2, offset2)).andReturn(false);

    EasyMock.replay(mockSupplier1, mockSupplier2, offset1, offset2);

    assertEquals(true, recordSupplierGroup.isOffsetAvailable(partition1, offset1));
    assertEquals(false, recordSupplierGroup.isOffsetAvailable(partition2, offset2));

    EasyMock.verify(mockSupplier1, mockSupplier2, offset1, offset2);
  }

  @Test
  public void testGetPosition()
  {
    StreamPartition<TestPartitionId> partition1 = new StreamPartition<>("stream-1", new TestPartitionId("cluster1"));
    StreamPartition<TestPartitionId> partition2 = new StreamPartition<>("stream-2", new TestPartitionId("cluster2"));

    EasyMock.expect(mockSupplier1.getPosition(partition1)).andReturn(50L);
    EasyMock.expect(mockSupplier2.getPosition(partition2)).andReturn(60L);

    EasyMock.replay(mockSupplier1, mockSupplier2);

    assertEquals(Long.valueOf(50), recordSupplierGroup.getPosition(partition1));
    assertEquals(Long.valueOf(60), recordSupplierGroup.getPosition(partition2));

    EasyMock.verify(mockSupplier1, mockSupplier2);
  }

  @Test
  public void testGetPartitionIds()
  {
    Set<TestPartitionId> partitionIds1 = new HashSet<>(Arrays.asList(
        new TestPartitionId("partition1"), new TestPartitionId("partition3")));
    Set<TestPartitionId> partitionIds2 = new HashSet<>(Arrays.asList(
        new TestPartitionId("partition2"), new TestPartitionId("partition4")));

    EasyMock.expect(mockSupplier1.getPartitionIds("stream")).andReturn(partitionIds1);
    EasyMock.expect(mockSupplier2.getPartitionIds("stream")).andReturn(partitionIds2);

    EasyMock.replay(mockSupplier1, mockSupplier2);

    Set<TestPartitionId> partitionIds = recordSupplierGroup.getPartitionIds("stream");

    assertEquals(4, partitionIds.size());

    EasyMock.verify(mockSupplier1, mockSupplier2);
  }

  @Test
  public void testClose()
  {
    mockSupplier1.close();
    mockSupplier2.close();

    EasyMock.replay(mockSupplier1, mockSupplier2);

    recordSupplierGroup.close();

    EasyMock.verify(mockSupplier1, mockSupplier2);
  }
}
