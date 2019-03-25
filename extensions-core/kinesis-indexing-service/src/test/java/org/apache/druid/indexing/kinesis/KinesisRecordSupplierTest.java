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

package org.apache.druid.indexing.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.easymock.Capture;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;

public class KinesisRecordSupplierTest extends EasyMockSupport
{
  private static final String stream = "stream";
  private static long poll_timeout_millis = 2000;
  private static int recordsPerFetch;
  private static String shardId1 = "1";
  private static String shardId0 = "0";
  private static String shard1Iterator = "1";
  private static String shard0Iterator = "0";
  private static AmazonKinesis kinesis;
  private static DescribeStreamResult describeStreamResult;
  private static GetShardIteratorResult getShardIteratorResult0;
  private static GetShardIteratorResult getShardIteratorResult1;
  private static GetRecordsResult getRecordsResult0;
  private static GetRecordsResult getRecordsResult1;
  private static StreamDescription streamDescription;
  private static Shard shard0;
  private static Shard shard1;
  private static KinesisRecordSupplier recordSupplier;
  private static List<Record> shard1Records = ImmutableList.of(
      new Record().withData(jb("2011", "d", "y", "10", "20.0", "1.0")).withSequenceNumber("0"),
      new Record().withData(jb("2011", "e", "y", "10", "20.0", "1.0")).withSequenceNumber("1"),
      new Record().withData(jb("246140482-04-24T15:36:27.903Z", "x", "z", "10", "20.0", "1.0")).withSequenceNumber("2"),
      new Record().withData(ByteBuffer.wrap(StringUtils.toUtf8("unparseable"))).withSequenceNumber("3"),
      new Record().withData(ByteBuffer.wrap(StringUtils.toUtf8("unparseable2"))).withSequenceNumber("4"),
      new Record().withData(ByteBuffer.wrap(StringUtils.toUtf8("{}"))).withSequenceNumber("5"),
      new Record().withData(jb("2013", "f", "y", "10", "20.0", "1.0")).withSequenceNumber("6"),
      new Record().withData(jb("2049", "f", "y", "notanumber", "20.0", "1.0")).withSequenceNumber("7"),
      new Record().withData(jb("2012", "g", "y", "10", "20.0", "1.0")).withSequenceNumber("8"),
      new Record().withData(jb("2011", "h", "y", "10", "20.0", "1.0")).withSequenceNumber("9")
  );
  private static List<Record> shard0Records = ImmutableList.of(
      new Record().withData(jb("2008", "a", "y", "10", "20.0", "1.0")).withSequenceNumber("0"),
      new Record().withData(jb("2009", "b", "y", "10", "20.0", "1.0")).withSequenceNumber("1")
  );
  private static List<Object> allRecords = ImmutableList.builder()
                                                        .addAll(shard0Records.stream()
                                                                             .map(x -> new OrderedPartitionableRecord<>(
                                                                                 stream,
                                                                                 shardId0,
                                                                                 x.getSequenceNumber(),
                                                                                 Collections
                                                                                     .singletonList(
                                                                                         toByteArray(
                                                                                             x.getData()))
                                                                             ))
                                                                             .collect(
                                                                                 Collectors
                                                                                     .toList()))
                                                        .addAll(shard1Records.stream()
                                                                             .map(x -> new OrderedPartitionableRecord<>(
                                                                                 stream,
                                                                                 shardId1,
                                                                                 x.getSequenceNumber(),
                                                                                 Collections
                                                                                     .singletonList(
                                                                                         toByteArray(
                                                                                             x.getData()))
                                                                             ))
                                                                             .collect(
                                                                                 Collectors
                                                                                     .toList()))
                                                        .build();

  private static ByteBuffer jb(String timestamp, String dim1, String dim2, String dimLong, String dimFloat, String met1)
  {
    try {
      return ByteBuffer.wrap(new ObjectMapper().writeValueAsBytes(
          ImmutableMap.builder()
                      .put("timestamp", timestamp)
                      .put("dim1", dim1)
                      .put("dim2", dim2)
                      .put("dimLong", dimLong)
                      .put("dimFloat", dimFloat)
                      .put("met1", met1)
                      .build()
      ));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setupTest()
  {
    kinesis = createMock(AmazonKinesisClient.class);
    describeStreamResult = createMock(DescribeStreamResult.class);
    getShardIteratorResult0 = createMock(GetShardIteratorResult.class);
    getShardIteratorResult1 = createMock(GetShardIteratorResult.class);
    getRecordsResult0 = createMock(GetRecordsResult.class);
    getRecordsResult1 = createMock(GetRecordsResult.class);
    streamDescription = createMock(StreamDescription.class);
    shard0 = createMock(Shard.class);
    shard1 = createMock(Shard.class);
    recordsPerFetch = 1;
  }

  @After
  public void tearDownTest()
  {
    recordSupplier.close();
    recordSupplier = null;
  }

  @Test
  public void testSupplierSetup()
  {
    Capture<String> captured = Capture.newInstance();
    expect(kinesis.describeStream(capture(captured))).andReturn(describeStreamResult).once();
    expect(describeStreamResult.getStreamDescription()).andReturn(streamDescription).once();
    expect(streamDescription.getShards()).andReturn(ImmutableList.of(shard0, shard1)).once();
    expect(shard0.getShardId()).andReturn(shardId0).once();
    expect(shard1.getShardId()).andReturn(shardId1).once();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(stream, shardId0),
        StreamPartition.of(stream, shardId1)
    );

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        recordsPerFetch,
        0,
        2,
        false,
        100,
        5000,
        5000,
        60000,
        5
    );

    Assert.assertTrue(recordSupplier.getAssignment().isEmpty());

    recordSupplier.assign(partitions);

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(ImmutableSet.of(shardId1, shardId0), recordSupplier.getPartitionIds(stream));
    Assert.assertEquals(Collections.emptyList(), recordSupplier.poll(100));

    verifyAll();
    Assert.assertEquals(stream, captured.getValue());
  }

  private static GetRecordsRequest generateGetRecordsReq(String shardIterator, int limit)
  {
    return new GetRecordsRequest().withShardIterator(shardIterator).withLimit(limit);
  }

  // filter out EOS markers
  private static List<OrderedPartitionableRecord<String, String>> cleanRecords(List<OrderedPartitionableRecord<String, String>> records)
  {
    return records.stream()
                  .filter(x -> !x.getSequenceNumber()
                                 .equals(KinesisSequenceNumber.END_OF_SHARD_MARKER))
                  .collect(Collectors.toList());
  }

  @Test
  public void testPoll() throws InterruptedException
  {
    recordsPerFetch = 100;

    expect(kinesis.getShardIterator(
        anyObject(),
        eq(shardId0),
        anyString(),
        anyString()
    )).andReturn(
        getShardIteratorResult0).anyTimes();

    expect(kinesis.getShardIterator(
        anyObject(),
        eq(shardId1),
        anyString(),
        anyString()
    )).andReturn(
        getShardIteratorResult1).anyTimes();

    expect(getShardIteratorResult0.getShardIterator()).andReturn(shard0Iterator).anyTimes();
    expect(getShardIteratorResult1.getShardIterator()).andReturn(shard1Iterator).anyTimes();
    expect(kinesis.getRecords(generateGetRecordsReq(shard0Iterator, recordsPerFetch))).andReturn(getRecordsResult0)
                                                                                      .anyTimes();
    expect(kinesis.getRecords(generateGetRecordsReq(shard1Iterator, recordsPerFetch))).andReturn(getRecordsResult1)
                                                                                      .anyTimes();
    expect(getRecordsResult0.getRecords()).andReturn(shard0Records).once();
    expect(getRecordsResult1.getRecords()).andReturn(shard1Records).once();
    expect(getRecordsResult0.getNextShardIterator()).andReturn(null).anyTimes();
    expect(getRecordsResult1.getNextShardIterator()).andReturn(null).anyTimes();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(stream, shardId0),
        StreamPartition.of(stream, shardId1)
    );


    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        recordsPerFetch,
        0,
        2,
        false,
        100,
        5000,
        5000,
        60000,
        100
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    recordSupplier.start();

    while (recordSupplier.bufferSize() < 12) {
      Thread.sleep(100);
    }

    List<OrderedPartitionableRecord<String, String>> polledRecords = cleanRecords(recordSupplier.poll(
        poll_timeout_millis));

    verifyAll();

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertTrue(polledRecords.containsAll(allRecords));
  }

  @Test
  public void testSeek()
      throws InterruptedException
  {
    recordsPerFetch = 100;

    expect(kinesis.getShardIterator(
        anyObject(),
        eq(shardId0),
        anyString(),
        anyString()
    )).andReturn(
        getShardIteratorResult0).anyTimes();

    expect(kinesis.getShardIterator(
        anyObject(),
        eq(shardId1),
        anyString(),
        anyString()
    )).andReturn(
        getShardIteratorResult1).anyTimes();

    expect(getShardIteratorResult0.getShardIterator()).andReturn(shard0Iterator).anyTimes();
    expect(getShardIteratorResult1.getShardIterator()).andReturn(shard1Iterator).anyTimes();
    expect(kinesis.getRecords(generateGetRecordsReq(shard0Iterator, recordsPerFetch))).andReturn(getRecordsResult0)
                                                                                      .anyTimes();
    expect(kinesis.getRecords(generateGetRecordsReq(shard1Iterator, recordsPerFetch))).andReturn(getRecordsResult1)
                                                                                      .anyTimes();
    expect(getRecordsResult0.getRecords()).andReturn(shard0Records.subList(1, shard0Records.size())).once();
    expect(getRecordsResult1.getRecords()).andReturn(shard1Records.subList(2, shard1Records.size())).once();
    expect(getRecordsResult0.getNextShardIterator()).andReturn(null).anyTimes();
    expect(getRecordsResult1.getNextShardIterator()).andReturn(null).anyTimes();

    replayAll();

    StreamPartition<String> shard0Partition = StreamPartition.of(stream, shardId0);
    StreamPartition<String> shard1Partition = StreamPartition.of(stream, shardId1);
    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        shard0Partition,
        shard1Partition
    );

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        recordsPerFetch,
        0,
        2,
        false,
        100,
        5000,
        5000,
        60000,
        100
    );

    recordSupplier.assign(partitions);
    recordSupplier.seek(shard1Partition, shard1Records.get(2).getSequenceNumber());
    recordSupplier.seek(shard0Partition, shard0Records.get(1).getSequenceNumber());
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 9; i++) {
      Thread.sleep(100);
    }

    List<OrderedPartitionableRecord<String, String>> polledRecords = cleanRecords(recordSupplier.poll(
        poll_timeout_millis));

    verifyAll();
    Assert.assertEquals(9, polledRecords.size());
    Assert.assertTrue(polledRecords.containsAll(allRecords.subList(4, 12)));
    Assert.assertTrue(polledRecords.containsAll(allRecords.subList(1, 2)));

  }


  @Test
  public void testSeekToLatest()
      throws InterruptedException
  {
    recordsPerFetch = 100;

    expect(kinesis.getShardIterator(
        anyObject(),
        eq(shardId0),
        anyString(),
        anyString()
    )).andReturn(
        getShardIteratorResult0).anyTimes();

    expect(kinesis.getShardIterator(
        anyObject(),
        eq(shardId1),
        anyString(),
        anyString()
    )).andReturn(
        getShardIteratorResult1).anyTimes();

    expect(getShardIteratorResult0.getShardIterator()).andReturn(null).once();
    expect(getShardIteratorResult1.getShardIterator()).andReturn(null).once();

    replayAll();

    StreamPartition<String> shard0 = StreamPartition.of(stream, shardId0);
    StreamPartition<String> shard1 = StreamPartition.of(stream, shardId1);
    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        shard0,
        shard1
    );

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        recordsPerFetch,
        0,
        2,
        false,
        100,
        5000,
        5000,
        60000,
        100
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToLatest(partitions);
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 2; i++) {
      Thread.sleep(100);
    }
    Assert.assertEquals(Collections.emptyList(), cleanRecords(recordSupplier.poll(poll_timeout_millis)));

    verifyAll();
  }

  @Test(expected = ISE.class)
  public void testSeekUnassigned() throws InterruptedException
  {
    StreamPartition<String> shard0 = StreamPartition.of(stream, shardId0);
    StreamPartition<String> shard1 = StreamPartition.of(stream, shardId1);
    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        shard1
    );

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        1,
        0,
        2,
        false,
        100,
        5000,
        5000,
        60000,
        5
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(Collections.singleton(shard0));
  }


  @Test
  public void testPollAfterSeek()
      throws InterruptedException
  {
    // tests that after doing a seek, the now invalid records in buffer is cleaned up properly
    recordsPerFetch = 100;

    expect(kinesis.getShardIterator(
        anyObject(),
        eq(shardId1),
        anyString(),
        eq("5")
    )).andReturn(
        getShardIteratorResult1).once();

    expect(kinesis.getShardIterator(anyObject(), eq(shardId1), anyString(), eq("7"))).andReturn(getShardIteratorResult0)
                                                                                     .once();

    expect(getShardIteratorResult1.getShardIterator()).andReturn(shard1Iterator).once();
    expect(getShardIteratorResult0.getShardIterator()).andReturn(shard0Iterator).once();
    expect(kinesis.getRecords(generateGetRecordsReq(shard1Iterator, recordsPerFetch))).andReturn(getRecordsResult1)
                                                                                      .once();
    expect(kinesis.getRecords(generateGetRecordsReq(shard0Iterator, recordsPerFetch))).andReturn(getRecordsResult0)
                                                                                      .once();
    expect(getRecordsResult1.getRecords()).andReturn(shard1Records.subList(5, shard1Records.size())).once();
    expect(getRecordsResult0.getRecords()).andReturn(shard1Records.subList(7, shard1Records.size())).once();
    expect(getRecordsResult1.getNextShardIterator()).andReturn(null).anyTimes();
    expect(getRecordsResult0.getNextShardIterator()).andReturn(null).anyTimes();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(stream, shardId1)
    );

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        recordsPerFetch,
        0,
        2,
        false,
        100,
        5000,
        5000,
        60000,
        1
    );

    recordSupplier.assign(partitions);
    recordSupplier.seek(StreamPartition.of(stream, shardId1), "5");
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 6; i++) {
      Thread.sleep(100);
    }

    OrderedPartitionableRecord<String, String> firstRecord = recordSupplier.poll(poll_timeout_millis).get(0);

    Assert.assertEquals(
        allRecords.get(7),
        firstRecord
    );

    recordSupplier.seek(StreamPartition.of(stream, shardId1), "7");
    recordSupplier.start();

    while (recordSupplier.bufferSize() < 4) {
      Thread.sleep(100);
    }

    OrderedPartitionableRecord<String, String> record2 = recordSupplier.poll(poll_timeout_millis).get(0);

    Assert.assertEquals(allRecords.get(9), record2);
    verifyAll();
  }


  @Test
  public void testPollDeaggregate() throws InterruptedException
  {
    recordsPerFetch = 100;

    expect(kinesis.getShardIterator(
        anyObject(),
        eq(shardId0),
        anyString(),
        anyString()
    )).andReturn(
        getShardIteratorResult0).anyTimes();

    expect(kinesis.getShardIterator(
        anyObject(),
        eq(shardId1),
        anyString(),
        anyString()
    )).andReturn(
        getShardIteratorResult1).anyTimes();

    expect(getShardIteratorResult0.getShardIterator()).andReturn(shard0Iterator).anyTimes();
    expect(getShardIteratorResult1.getShardIterator()).andReturn(shard1Iterator).anyTimes();
    expect(kinesis.getRecords(generateGetRecordsReq(shard0Iterator, recordsPerFetch))).andReturn(getRecordsResult0)
                                                                                      .anyTimes();
    expect(kinesis.getRecords(generateGetRecordsReq(shard1Iterator, recordsPerFetch))).andReturn(getRecordsResult1)
                                                                                      .anyTimes();
    expect(getRecordsResult0.getRecords()).andReturn(shard0Records).once();
    expect(getRecordsResult1.getRecords()).andReturn(shard1Records).once();
    expect(getRecordsResult0.getNextShardIterator()).andReturn(null).anyTimes();
    expect(getRecordsResult1.getNextShardIterator()).andReturn(null).anyTimes();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(stream, shardId0),
        StreamPartition.of(stream, shardId1)
    );


    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        recordsPerFetch,
        0,
        2,
        true,
        100,
        5000,
        5000,
        60000,
        100
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 12; i++) {
      Thread.sleep(100);
    }

    List<OrderedPartitionableRecord<String, String>> polledRecords = cleanRecords(recordSupplier.poll(
        poll_timeout_millis));

    verifyAll();

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertTrue(polledRecords.containsAll(allRecords));
  }

  /**
   * Returns an array with the content between the position and limit of "buffer". This may be the buffer's backing
   * array itself. Does not modify position or limit of the buffer.
   */
  private static byte[] toByteArray(final ByteBuffer buffer)
  {
    if (buffer.hasArray()
        && buffer.arrayOffset() == 0
        && buffer.position() == 0
        && buffer.array().length == buffer.limit()) {
      return buffer.array();
    } else {
      final byte[] retVal = new byte[buffer.remaining()];
      buffer.duplicate().get(retVal);
      return retVal;
    }
  }
}
