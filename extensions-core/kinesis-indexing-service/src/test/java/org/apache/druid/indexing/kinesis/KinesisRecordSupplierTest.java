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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
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
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KinesisRecordSupplierTest extends EasyMockSupport
{
  private static final String STREAM = "stream";
  private static final long POLL_TIMEOUT_MILLIS = 2000;
  private static final String SHARD_ID1 = "1";
  private static final String SHARD_ID0 = "0";
  private static final String SHARD1_ITERATOR = "1";
  private static final String SHARD0_ITERATOR = "0";

  private static final Long SHARD0_LAG_MILLIS = 100L;
  private static final Long SHARD1_LAG_MILLIS = 200L;
  private static Map<String, Long> SHARDS_LAG_MILLIS =
      ImmutableMap.of(SHARD_ID0, SHARD0_LAG_MILLIS, SHARD_ID1, SHARD1_LAG_MILLIS);
  private static final List<Record> SHARD0_RECORDS = ImmutableList.of(
      new Record().withData(jb("2008", "a", "y", "10", "20.0", "1.0")).withSequenceNumber("0"),
      new Record().withData(jb("2009", "b", "y", "10", "20.0", "1.0")).withSequenceNumber("1")
  );
  private static final List<Record> SHARD1_RECORDS = ImmutableList.of(
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
  private static final List<Object> ALL_RECORDS = ImmutableList.builder()
                                                               .addAll(SHARD0_RECORDS.stream()
                                                                                     .map(x -> new OrderedPartitionableRecord<>(
                                                                                         STREAM,
                                                                                         SHARD_ID0,
                                                                                         x.getSequenceNumber(),
                                                                                         Collections
                                                                                             .singletonList(
                                                                                                 toByteArray(
                                                                                                     x.getData()))
                                                                                     ))
                                                                                     .collect(
                                                                                         Collectors
                                                                                             .toList()))
                                                               .addAll(SHARD1_RECORDS.stream()
                                                                                     .map(x -> new OrderedPartitionableRecord<>(
                                                                                         STREAM,
                                                                                         SHARD_ID1,
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

  private static int recordsPerFetch;
  private static AmazonKinesis kinesis;
  private static DescribeStreamResult describeStreamResult0;
  private static DescribeStreamResult describeStreamResult1;
  private static GetShardIteratorResult getShardIteratorResult0;
  private static GetShardIteratorResult getShardIteratorResult1;
  private static GetRecordsResult getRecordsResult0;
  private static GetRecordsResult getRecordsResult1;
  private static StreamDescription streamDescription0;
  private static StreamDescription streamDescription1;
  private static Shard shard0;
  private static Shard shard1;
  private static KinesisRecordSupplier recordSupplier;

  @Before
  public void setupTest()
  {
    kinesis = createMock(AmazonKinesisClient.class);
    describeStreamResult0 = createMock(DescribeStreamResult.class);
    describeStreamResult1 = createMock(DescribeStreamResult.class);
    getShardIteratorResult0 = createMock(GetShardIteratorResult.class);
    getShardIteratorResult1 = createMock(GetShardIteratorResult.class);
    getRecordsResult0 = createMock(GetRecordsResult.class);
    getRecordsResult1 = createMock(GetRecordsResult.class);
    streamDescription0 = createMock(StreamDescription.class);
    streamDescription1 = createMock(StreamDescription.class);
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
    final Capture<DescribeStreamRequest> capturedRequest = Capture.newInstance();

    EasyMock.expect(kinesis.describeStream(EasyMock.capture(capturedRequest))).andReturn(describeStreamResult0).once();
    EasyMock.expect(describeStreamResult0.getStreamDescription()).andReturn(streamDescription0).once();
    EasyMock.expect(streamDescription0.getShards()).andReturn(ImmutableList.of(shard0)).once();
    EasyMock.expect(streamDescription0.isHasMoreShards()).andReturn(true).once();
    EasyMock.expect(shard0.getShardId()).andReturn(SHARD_ID0).times(2);
    EasyMock.expect(kinesis.describeStream(EasyMock.anyObject(DescribeStreamRequest.class)))
            .andReturn(describeStreamResult1)
            .once();
    EasyMock.expect(describeStreamResult1.getStreamDescription()).andReturn(streamDescription1).once();
    EasyMock.expect(streamDescription1.getShards()).andReturn(ImmutableList.of(shard1)).once();
    EasyMock.expect(streamDescription1.isHasMoreShards()).andReturn(false).once();
    EasyMock.expect(shard1.getShardId()).andReturn(SHARD_ID1).once();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(STREAM, SHARD_ID0),
        StreamPartition.of(STREAM, SHARD_ID1)
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
        5,
        true
    );

    Assert.assertTrue(recordSupplier.getAssignment().isEmpty());

    recordSupplier.assign(partitions);

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(ImmutableSet.of(SHARD_ID1, SHARD_ID0), recordSupplier.getPartitionIds(STREAM));

    // calling poll would start background fetch if seek was called, but will instead be skipped and the results
    // empty
    Assert.assertEquals(Collections.emptyList(), recordSupplier.poll(100));

    verifyAll();

    final DescribeStreamRequest expectedRequest = new DescribeStreamRequest();
    expectedRequest.setStreamName(STREAM);
    expectedRequest.setExclusiveStartShardId("0");
    Assert.assertEquals(expectedRequest, capturedRequest.getValue());
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

    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID0),
        EasyMock.anyString(),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult0).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID1),
        EasyMock.anyString(),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult1).anyTimes();

    EasyMock.expect(getShardIteratorResult0.getShardIterator()).andReturn(SHARD0_ITERATOR).anyTimes();
    EasyMock.expect(getShardIteratorResult1.getShardIterator()).andReturn(SHARD1_ITERATOR).anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR, recordsPerFetch)))
            .andReturn(getRecordsResult0)
            .anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD1_ITERATOR, recordsPerFetch)))
            .andReturn(getRecordsResult1)
            .anyTimes();
    EasyMock.expect(getRecordsResult0.getRecords()).andReturn(SHARD0_RECORDS).once();
    EasyMock.expect(getRecordsResult1.getRecords()).andReturn(SHARD1_RECORDS).once();
    EasyMock.expect(getRecordsResult0.getNextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult1.getNextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult0.getMillisBehindLatest()).andReturn(SHARD0_LAG_MILLIS).once();
    EasyMock.expect(getRecordsResult1.getMillisBehindLatest()).andReturn(SHARD1_LAG_MILLIS).once();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(STREAM, SHARD_ID0),
        StreamPartition.of(STREAM, SHARD_ID1)
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
        100,
        true
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    recordSupplier.start();

    while (recordSupplier.bufferSize() < 12) {
      Thread.sleep(100);
    }

    List<OrderedPartitionableRecord<String, String>> polledRecords = cleanRecords(recordSupplier.poll(
        POLL_TIMEOUT_MILLIS));

    verifyAll();

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertTrue(polledRecords.containsAll(ALL_RECORDS));
    Assert.assertEquals(SHARDS_LAG_MILLIS, recordSupplier.getPartitionResourcesTimeLag());
  }

  @Test
  public void testPollWithKinesisInternalFailure() throws InterruptedException
  {
    recordsPerFetch = 100;

    EasyMock.expect(kinesis.getShardIterator(
            EasyMock.anyObject(),
            EasyMock.eq(SHARD_ID0),
            EasyMock.anyString(),
            EasyMock.anyString()
    )).andReturn(
            getShardIteratorResult0).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
            EasyMock.anyObject(),
            EasyMock.eq(SHARD_ID1),
            EasyMock.anyString(),
            EasyMock.anyString()
    )).andReturn(
            getShardIteratorResult1).anyTimes();

    EasyMock.expect(getShardIteratorResult0.getShardIterator()).andReturn(SHARD0_ITERATOR).anyTimes();
    EasyMock.expect(getShardIteratorResult1.getShardIterator()).andReturn(SHARD1_ITERATOR).anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR, recordsPerFetch)))
            .andReturn(getRecordsResult0)
            .anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD1_ITERATOR, recordsPerFetch)))
            .andReturn(getRecordsResult1)
            .anyTimes();
    AmazonServiceException getException = new AmazonServiceException("InternalFailure");
    getException.setErrorCode("InternalFailure");
    getException.setStatusCode(500);
    getException.setServiceName("AmazonKinesis");
    EasyMock.expect(getRecordsResult0.getRecords()).andThrow(getException).once();
    EasyMock.expect(getRecordsResult0.getRecords()).andReturn(SHARD0_RECORDS).once();
    AmazonServiceException getException2 = new AmazonServiceException("InternalFailure");
    getException2.setErrorCode("InternalFailure");
    getException2.setStatusCode(503);
    getException2.setServiceName("AmazonKinesis");
    EasyMock.expect(getRecordsResult1.getRecords()).andThrow(getException2).once();
    EasyMock.expect(getRecordsResult1.getRecords()).andReturn(SHARD1_RECORDS).once();
    EasyMock.expect(getRecordsResult0.getNextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult1.getNextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult0.getMillisBehindLatest()).andReturn(SHARD0_LAG_MILLIS).once();
    EasyMock.expect(getRecordsResult0.getMillisBehindLatest()).andReturn(SHARD0_LAG_MILLIS).once();
    EasyMock.expect(getRecordsResult1.getMillisBehindLatest()).andReturn(SHARD1_LAG_MILLIS).once();
    EasyMock.expect(getRecordsResult1.getMillisBehindLatest()).andReturn(SHARD1_LAG_MILLIS).once();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
            StreamPartition.of(STREAM, SHARD_ID0),
            StreamPartition.of(STREAM, SHARD_ID1)
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
            100,
            true
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    recordSupplier.start();

    while (recordSupplier.bufferSize() < 14) {
      Thread.sleep(100);
    }

    List<OrderedPartitionableRecord<String, String>> polledRecords = cleanRecords(recordSupplier.poll(
            POLL_TIMEOUT_MILLIS));

    verifyAll();

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertTrue(polledRecords.containsAll(ALL_RECORDS));
    Assert.assertEquals(SHARDS_LAG_MILLIS, recordSupplier.getPartitionResourcesTimeLag());
  }

  @Test
  public void testSeek()
      throws InterruptedException
  {
    recordsPerFetch = 100;

    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID0),
        EasyMock.anyString(),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult0).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID1),
        EasyMock.anyString(),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult1).anyTimes();

    EasyMock.expect(getShardIteratorResult0.getShardIterator()).andReturn(SHARD0_ITERATOR).anyTimes();
    EasyMock.expect(getShardIteratorResult1.getShardIterator()).andReturn(SHARD1_ITERATOR).anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR, recordsPerFetch)))
            .andReturn(getRecordsResult0)
            .anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD1_ITERATOR, recordsPerFetch)))
            .andReturn(getRecordsResult1)
            .anyTimes();
    EasyMock.expect(getRecordsResult0.getRecords()).andReturn(SHARD0_RECORDS.subList(1, SHARD0_RECORDS.size())).once();
    EasyMock.expect(getRecordsResult1.getRecords()).andReturn(SHARD1_RECORDS.subList(2, SHARD1_RECORDS.size())).once();
    EasyMock.expect(getRecordsResult0.getNextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult1.getNextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult0.getMillisBehindLatest()).andReturn(SHARD0_LAG_MILLIS).once();
    EasyMock.expect(getRecordsResult1.getMillisBehindLatest()).andReturn(SHARD1_LAG_MILLIS).once();

    replayAll();

    StreamPartition<String> shard0Partition = StreamPartition.of(STREAM, SHARD_ID0);
    StreamPartition<String> shard1Partition = StreamPartition.of(STREAM, SHARD_ID1);
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
        100,
        true
    );

    recordSupplier.assign(partitions);
    recordSupplier.seek(shard1Partition, SHARD1_RECORDS.get(2).getSequenceNumber());
    recordSupplier.seek(shard0Partition, SHARD0_RECORDS.get(1).getSequenceNumber());
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 9; i++) {
      Thread.sleep(100);
    }

    List<OrderedPartitionableRecord<String, String>> polledRecords = cleanRecords(recordSupplier.poll(
        POLL_TIMEOUT_MILLIS));

    verifyAll();
    Assert.assertEquals(9, polledRecords.size());
    Assert.assertTrue(polledRecords.containsAll(ALL_RECORDS.subList(4, 12)));
    Assert.assertTrue(polledRecords.containsAll(ALL_RECORDS.subList(1, 2)));
    Assert.assertEquals(SHARDS_LAG_MILLIS, recordSupplier.getPartitionResourcesTimeLag());
  }


  @Test
  public void testSeekToLatest()
      throws InterruptedException
  {
    recordsPerFetch = 100;

    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID0),
        EasyMock.anyString(),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult0).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID1),
        EasyMock.anyString(),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult1).anyTimes();

    EasyMock.expect(getShardIteratorResult0.getShardIterator()).andReturn(null).once();
    EasyMock.expect(getShardIteratorResult1.getShardIterator()).andReturn(null).once();

    replayAll();

    StreamPartition<String> shard0 = StreamPartition.of(STREAM, SHARD_ID0);
    StreamPartition<String> shard1 = StreamPartition.of(STREAM, SHARD_ID1);
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
        100,
        true
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToLatest(partitions);
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 2; i++) {
      Thread.sleep(100);
    }
    Assert.assertEquals(Collections.emptyList(), cleanRecords(recordSupplier.poll(POLL_TIMEOUT_MILLIS)));

    verifyAll();
  }

  @Test(expected = ISE.class)
  public void testSeekUnassigned() throws InterruptedException
  {
    StreamPartition<String> shard0 = StreamPartition.of(STREAM, SHARD_ID0);
    StreamPartition<String> shard1 = StreamPartition.of(STREAM, SHARD_ID1);
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
        5,
        true
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

    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID1),
        EasyMock.anyString(),
        EasyMock.eq("5")
    )).andReturn(
        getShardIteratorResult1).once();

    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID1),
        EasyMock.anyString(),
        EasyMock.eq("7")
    )).andReturn(getShardIteratorResult0)
            .once();

    EasyMock.expect(getShardIteratorResult1.getShardIterator()).andReturn(SHARD1_ITERATOR).once();
    EasyMock.expect(getShardIteratorResult0.getShardIterator()).andReturn(SHARD0_ITERATOR).once();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD1_ITERATOR, recordsPerFetch)))
            .andReturn(getRecordsResult1)
            .once();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR, recordsPerFetch)))
            .andReturn(getRecordsResult0)
            .once();
    EasyMock.expect(getRecordsResult1.getRecords()).andReturn(SHARD1_RECORDS.subList(5, SHARD1_RECORDS.size())).once();
    EasyMock.expect(getRecordsResult0.getRecords()).andReturn(SHARD1_RECORDS.subList(7, SHARD1_RECORDS.size())).once();
    EasyMock.expect(getRecordsResult1.getNextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult0.getNextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult0.getMillisBehindLatest()).andReturn(SHARD0_LAG_MILLIS).once();
    EasyMock.expect(getRecordsResult1.getMillisBehindLatest()).andReturn(SHARD1_LAG_MILLIS).once();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(STREAM, SHARD_ID1)
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
        1,
        true
    );

    recordSupplier.assign(partitions);
    recordSupplier.seek(StreamPartition.of(STREAM, SHARD_ID1), "5");
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 6; i++) {
      Thread.sleep(100);
    }

    OrderedPartitionableRecord<String, String> firstRecord = recordSupplier.poll(POLL_TIMEOUT_MILLIS).get(0);

    Assert.assertEquals(
        ALL_RECORDS.get(7),
        firstRecord
    );

    // only one partition in this test. first results come from getRecordsResult1, which has SHARD1_LAG_MILLIS
    Assert.assertEquals(ImmutableMap.of(SHARD_ID1, SHARD1_LAG_MILLIS), recordSupplier.getPartitionResourcesTimeLag());

    recordSupplier.seek(StreamPartition.of(STREAM, SHARD_ID1), "7");
    recordSupplier.start();

    while (recordSupplier.bufferSize() < 4) {
      Thread.sleep(100);
    }


    OrderedPartitionableRecord<String, String> record2 = recordSupplier.poll(POLL_TIMEOUT_MILLIS).get(0);

    Assert.assertEquals(ALL_RECORDS.get(9), record2);
    // only one partition in this test. second results come from getRecordsResult0, which has SHARD0_LAG_MILLIS
    Assert.assertEquals(ImmutableMap.of(SHARD_ID1, SHARD0_LAG_MILLIS), recordSupplier.getPartitionResourcesTimeLag());
    verifyAll();
  }


  @Test
  public void testPollDeaggregate() throws InterruptedException
  {
    recordsPerFetch = 100;

    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID0),
        EasyMock.anyString(),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult0).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID1),
        EasyMock.anyString(),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult1).anyTimes();

    EasyMock.expect(getShardIteratorResult0.getShardIterator()).andReturn(SHARD0_ITERATOR).anyTimes();
    EasyMock.expect(getShardIteratorResult1.getShardIterator()).andReturn(SHARD1_ITERATOR).anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR, recordsPerFetch)))
            .andReturn(getRecordsResult0)
            .anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD1_ITERATOR, recordsPerFetch)))
            .andReturn(getRecordsResult1)
            .anyTimes();
    EasyMock.expect(getRecordsResult0.getRecords()).andReturn(SHARD0_RECORDS).once();
    EasyMock.expect(getRecordsResult1.getRecords()).andReturn(SHARD1_RECORDS).once();
    EasyMock.expect(getRecordsResult0.getNextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult1.getNextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult0.getMillisBehindLatest()).andReturn(SHARD0_LAG_MILLIS).once();
    EasyMock.expect(getRecordsResult1.getMillisBehindLatest()).andReturn(SHARD1_LAG_MILLIS).once();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(STREAM, SHARD_ID0),
        StreamPartition.of(STREAM, SHARD_ID1)
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
        100,
        true
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 12; i++) {
      Thread.sleep(100);
    }

    List<OrderedPartitionableRecord<String, String>> polledRecords = cleanRecords(recordSupplier.poll(
        POLL_TIMEOUT_MILLIS));

    verifyAll();

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertTrue(polledRecords.containsAll(ALL_RECORDS));
    Assert.assertEquals(SHARDS_LAG_MILLIS, recordSupplier.getPartitionResourcesTimeLag());
  }

  @Test
  public void getLatestSequenceNumberWhenShardIsEmptyShouldReturnsNull()
  {

    KinesisRecordSupplier recordSupplier = getSequenceNumberWhenShardIsEmptyShouldReturnsNullHelper();
    Assert.assertNull(recordSupplier.getLatestSequenceNumber(StreamPartition.of(STREAM, SHARD_ID0)));
    verifyAll();
  }

  @Test
  public void getEarliestSequenceNumberWhenShardIsEmptyShouldReturnsNull()
  {

    KinesisRecordSupplier recordSupplier = getSequenceNumberWhenShardIsEmptyShouldReturnsNullHelper();
    Assert.assertNull(recordSupplier.getEarliestSequenceNumber(StreamPartition.of(STREAM, SHARD_ID0)));
    verifyAll();
  }

  private KinesisRecordSupplier getSequenceNumberWhenShardIsEmptyShouldReturnsNullHelper()
  {
    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.eq(STREAM),
        EasyMock.eq(SHARD_ID0),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult0).anyTimes();

    EasyMock.expect(getShardIteratorResult0.getShardIterator()).andReturn(SHARD0_ITERATOR).anyTimes();

    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR, 1000)))
            .andReturn(getRecordsResult0)
            .times(1, Integer.MAX_VALUE);

    EasyMock.expect(getRecordsResult0.getRecords()).andReturn(Collections.emptyList()).times(1, Integer.MAX_VALUE);
    EasyMock.expect(getRecordsResult0.getNextShardIterator()).andReturn(SHARD0_ITERATOR).times(1, Integer.MAX_VALUE);
    EasyMock.expect(getRecordsResult0.getMillisBehindLatest()).andReturn(0L).once();

    replayAll();

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        recordsPerFetch,
        0,
        2,
        true,
        100,
        5000,
        5000,
        1000,
        100,
        true
    );
    return recordSupplier;
  }

  @Test
  public void getPartitionTimeLag() throws InterruptedException
  {
    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID0),
        EasyMock.anyString(),
        EasyMock.anyString()
    )).andReturn(getShardIteratorResult0).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID1),
        EasyMock.anyString(),
        EasyMock.anyString()
    )).andReturn(getShardIteratorResult1).anyTimes();

    EasyMock.expect(getShardIteratorResult0.getShardIterator()).andReturn(SHARD0_ITERATOR).anyTimes();
    EasyMock.expect(getShardIteratorResult1.getShardIterator()).andReturn(SHARD1_ITERATOR).anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR, recordsPerFetch)))
            .andReturn(getRecordsResult0)
            .anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD1_ITERATOR, recordsPerFetch)))
            .andReturn(getRecordsResult1)
            .anyTimes();
    EasyMock.expect(getRecordsResult0.getRecords()).andReturn(SHARD0_RECORDS).once();
    EasyMock.expect(getRecordsResult1.getRecords()).andReturn(SHARD1_RECORDS).once();
    EasyMock.expect(getRecordsResult0.getNextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult1.getNextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult0.getMillisBehindLatest()).andReturn(SHARD0_LAG_MILLIS).times(2);
    EasyMock.expect(getRecordsResult1.getMillisBehindLatest()).andReturn(SHARD1_LAG_MILLIS).times(2);

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(STREAM, SHARD_ID0),
        StreamPartition.of(STREAM, SHARD_ID1)
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
        100,
        true
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 12; i++) {
      Thread.sleep(100);
    }

    Map<String, Long> timeLag = recordSupplier.getPartitionResourcesTimeLag();


    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(SHARDS_LAG_MILLIS, timeLag);

    Map<String, String> offsts = ImmutableMap.of(
        SHARD_ID1, SHARD1_RECORDS.get(0).getSequenceNumber(),
        SHARD_ID0, SHARD0_RECORDS.get(0).getSequenceNumber()
    );
    Map<String, Long> independentTimeLag = recordSupplier.getPartitionsTimeLag(STREAM, offsts);
    Assert.assertEquals(SHARDS_LAG_MILLIS, independentTimeLag);
    verifyAll();
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
