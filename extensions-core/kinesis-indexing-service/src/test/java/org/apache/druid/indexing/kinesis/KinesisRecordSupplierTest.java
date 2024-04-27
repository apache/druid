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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.impl.ByteEntity;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.druid.indexing.kinesis.KinesisSequenceNumber.END_OF_SHARD_MARKER;
import static org.apache.druid.indexing.kinesis.KinesisSequenceNumber.EXPIRED_MARKER;
import static org.apache.druid.indexing.kinesis.KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER;
import static org.apache.druid.indexing.kinesis.KinesisSequenceNumber.UNREAD_LATEST;
import static org.apache.druid.indexing.kinesis.KinesisSequenceNumber.UNREAD_TRIM_HORIZON;

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
  private static final Long SHARD1_LAG_MILLIS_EMPTY = 0L;
  private static Map<String, Long> SHARDS_LAG_MILLIS =
      ImmutableMap.of(SHARD_ID0, SHARD0_LAG_MILLIS, SHARD_ID1, SHARD1_LAG_MILLIS);
  private static Map<String, Long> SHARDS_LAG_MILLIS_EMPTY =
          ImmutableMap.of(SHARD_ID0, SHARD0_LAG_MILLIS, SHARD_ID1, SHARD1_LAG_MILLIS_EMPTY);
  private static final List<Record> SHARD0_RECORDS = ImmutableList.of(
      new Record().withData(jb("2008", "a", "y", "10", "20.0", "1.0")).withSequenceNumber("0"),
      new Record().withData(jb("2009", "b", "y", "10", "20.0", "1.0")).withSequenceNumber("1")
  );
  private static final List<Record> SHARD1_RECORDS_EMPTY = ImmutableList.of();
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
  private static final List<OrderedPartitionableRecord<String, String, ByteEntity>> ALL_RECORDS = ImmutableList.<OrderedPartitionableRecord<String, String, ByteEntity>>builder()
                                                               .addAll(SHARD0_RECORDS.stream()
                                                                                     .map(x -> new OrderedPartitionableRecord<>(
                                                                                         STREAM,
                                                                                         SHARD_ID0,
                                                                                         x.getSequenceNumber(),
                                                                                         Collections
                                                                                             .singletonList(
                                                                                                 new ByteEntity(
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
                                                                                                 new ByteEntity(
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
  private static AmazonKinesis kinesis;
  private static ListShardsResult listShardsResult0;
  private static ListShardsResult listShardsResult1;
  private static GetShardIteratorResult getShardIteratorResult0;
  private static GetShardIteratorResult getShardIteratorResult1;
  private static DescribeStreamResult describeStreamResult0;
  private static DescribeStreamResult describeStreamResult1;
  private static StreamDescription streamDescription0;
  private static StreamDescription streamDescription1;
  private static GetRecordsResult getRecordsResult0;
  private static GetRecordsResult getRecordsResult1;
  private static Shard shard0;
  private static Shard shard1;
  private static KinesisRecordSupplier recordSupplier;

  @Before
  public void setupTest()
  {
    kinesis = createMock(AmazonKinesisClient.class);
    listShardsResult0 = createMock(ListShardsResult.class);
    listShardsResult1 = createMock(ListShardsResult.class);
    describeStreamResult0 = createMock(DescribeStreamResult.class);
    describeStreamResult1 = createMock(DescribeStreamResult.class);
    streamDescription0 = createMock(StreamDescription.class);
    streamDescription1 = createMock(StreamDescription.class);
    getShardIteratorResult0 = createMock(GetShardIteratorResult.class);
    getShardIteratorResult1 = createMock(GetShardIteratorResult.class);
    getRecordsResult0 = createMock(GetRecordsResult.class);
    getRecordsResult1 = createMock(GetRecordsResult.class);
    shard0 = createMock(Shard.class);
    shard1 = createMock(Shard.class);
  }

  @After
  public void tearDownTest()
  {
    if (null != recordSupplier) {
      recordSupplier.close();
    }
    recordSupplier = null;
  }

  @Test
  public void testSupplierSetup_withoutListShards()
  {
    final Capture<DescribeStreamRequest> capturedRequest0 = Capture.newInstance();
    final Capture<DescribeStreamRequest> capturedRequest1 = Capture.newInstance();

    EasyMock.expect(kinesis.describeStream(EasyMock.capture(capturedRequest0))).andReturn(describeStreamResult0).once();
    EasyMock.expect(describeStreamResult0.getStreamDescription()).andReturn(streamDescription0).once();
    EasyMock.expect(streamDescription0.getShards()).andReturn(ImmutableList.of(shard0, shard1)).once();
    EasyMock.expect(shard0.getShardId()).andReturn(SHARD_ID0).once();
    EasyMock.expect(shard1.getShardId()).andReturn(SHARD_ID1).times(2);
    EasyMock.expect(streamDescription0.isHasMoreShards()).andReturn(true).once();

    EasyMock.expect(kinesis.describeStream(EasyMock.capture(capturedRequest1))).andReturn(describeStreamResult1).once();
    EasyMock.expect(describeStreamResult1.getStreamDescription()).andReturn(streamDescription1).once();
    EasyMock.expect(streamDescription1.getShards()).andReturn(ImmutableList.of()).once();
    EasyMock.expect(streamDescription1.isHasMoreShards()).andReturn(false).once();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(STREAM, SHARD_ID0),
        StreamPartition.of(STREAM, SHARD_ID1)
    );

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        0,
        2,
        100,
        5000,
        5000,
        1_000_000,
        true,
        false
    );

    Assert.assertTrue(recordSupplier.getAssignment().isEmpty());

    recordSupplier.assign(partitions);

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(ImmutableSet.of(SHARD_ID0, SHARD_ID1), recordSupplier.getPartitionIds(STREAM));

    // calling poll would start background fetch if seek was called, but will instead be skipped and the results
    // empty
    Assert.assertEquals(Collections.emptyList(), recordSupplier.poll(100));

    verifyAll();

    // Since the same request is modified, every captured argument will be the same at the end
    Assert.assertEquals(capturedRequest0.getValues(), capturedRequest1.getValues());

    final DescribeStreamRequest expectedRequest = new DescribeStreamRequest();
    expectedRequest.setStreamName(STREAM);
    expectedRequest.setExclusiveStartShardId(SHARD_ID1);
    Assert.assertEquals(expectedRequest, capturedRequest1.getValue());
  }

  @Test
  public void testSupplierSetup_withListShards()
  {
    final Capture<ListShardsRequest> capturedRequest0 = Capture.newInstance();
    final Capture<ListShardsRequest> capturedRequest1 = Capture.newInstance();

    EasyMock.expect(kinesis.listShards(EasyMock.capture(capturedRequest0))).andReturn(listShardsResult0).once();
    EasyMock.expect(listShardsResult0.getShards()).andReturn(ImmutableList.of(shard0)).once();
    String nextToken = "nextToken";
    EasyMock.expect(listShardsResult0.getNextToken()).andReturn(nextToken).once();
    EasyMock.expect(shard0.getShardId()).andReturn(SHARD_ID0).once();
    EasyMock.expect(kinesis.listShards(EasyMock.capture(capturedRequest1))).andReturn(listShardsResult1).once();
    EasyMock.expect(listShardsResult1.getShards()).andReturn(ImmutableList.of(shard1)).once();
    EasyMock.expect(listShardsResult1.getNextToken()).andReturn(null).once();
    EasyMock.expect(shard1.getShardId()).andReturn(SHARD_ID1).once();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(STREAM, SHARD_ID0),
        StreamPartition.of(STREAM, SHARD_ID1)
    );

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        0,
        2,
        100,
        5000,
        5000,
        1_000_000,
        true,
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

    final ListShardsRequest expectedRequest0 = new ListShardsRequest();
    expectedRequest0.setStreamName(STREAM);
    Assert.assertEquals(expectedRequest0, capturedRequest0.getValue());

    final ListShardsRequest expectedRequest1 = new ListShardsRequest();
    expectedRequest1.setNextToken(nextToken);
    Assert.assertEquals(expectedRequest1, capturedRequest1.getValue());
  }

  private static GetRecordsRequest generateGetRecordsReq(String shardIterator)
  {
    return new GetRecordsRequest().withShardIterator(shardIterator);
  }

  private static GetRecordsRequest generateGetRecordsWithLimitReq(String shardIterator, int limit)
  {
    return new GetRecordsRequest().withShardIterator(shardIterator).withLimit(limit);
  }

  // filter out EOS markers
  private static List<OrderedPartitionableRecord<String, String, ByteEntity>> cleanRecords(List<OrderedPartitionableRecord<String, String, ByteEntity>> records)
  {
    return records.stream()
                  .filter(x -> !x.getSequenceNumber()
                                 .equals(END_OF_SHARD_MARKER))
                  .collect(Collectors.toList());
  }

  @Test
  public void testPollWithKinesisInternalFailure() throws InterruptedException
  {
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
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR)))
            .andReturn(getRecordsResult0)
            .anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD1_ITERATOR)))
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
            0,
            2,
            10_000,
            5000,
            5000,
        1_000_000,
            true,
            false
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    recordSupplier.start();

    while (recordSupplier.bufferSize() < 14) {
      Thread.sleep(100);
    }

    List<OrderedPartitionableRecord<String, String, ByteEntity>> polledRecords = cleanRecords(recordSupplier.poll(
            POLL_TIMEOUT_MILLIS));

    verifyAll();

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertTrue(polledRecords.containsAll(ALL_RECORDS));
    Assert.assertEquals(SHARDS_LAG_MILLIS, recordSupplier.getPartitionResourcesTimeLag());
  }

  @Test
  public void testPollWithKinesisNonRetryableFailure() throws InterruptedException
  {
    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID0),
        EasyMock.anyString(),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult0).anyTimes();

    AmazonServiceException getException = new AmazonServiceException("BadRequest");
    getException.setErrorCode("BadRequest");
    getException.setStatusCode(400);
    getException.setServiceName("AmazonKinesis");
    EasyMock.expect(getShardIteratorResult0.getShardIterator()).andReturn(SHARD0_ITERATOR).anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR)))
            .andThrow(getException)
            .once();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(STREAM, SHARD_ID0)
    );


    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        0,
        1,
        100,
        5000,
        5000,
        1_000_000,
        true,
        false
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    recordSupplier.start();

    int count = 0;
    while (recordSupplier.isAnyFetchActive() && count++ < 10) {
      Thread.sleep(100);
    }
    Assert.assertFalse(recordSupplier.isAnyFetchActive());

    List<OrderedPartitionableRecord<String, String, ByteEntity>> polledRecords = cleanRecords(recordSupplier.poll(
        POLL_TIMEOUT_MILLIS));

    verifyAll();

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(0, polledRecords.size());
  }

  @Test
  public void testSeek()
      throws InterruptedException
  {
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
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR)))
            .andReturn(getRecordsResult0)
            .anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD1_ITERATOR)))
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
        0,
        2,
        10_000,
        5000,
        5000,
        1_000_000,
        true,
        false
    );

    recordSupplier.assign(partitions);
    recordSupplier.seek(shard1Partition, SHARD1_RECORDS.get(2).getSequenceNumber());
    recordSupplier.seek(shard0Partition, SHARD0_RECORDS.get(1).getSequenceNumber());
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 9; i++) {
      Thread.sleep(100);
    }

    List<OrderedPartitionableRecord<String, String, ByteEntity>> polledRecords = cleanRecords(recordSupplier.poll(
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
        0,
        2,
        100,
        5000,
        5000,
        1_000_000,
        true,
        false
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
        0,
        2,
        100,
        5000,
        5000,
        1_000_000,
        true,
        false
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(Collections.singleton(shard0));
  }


  @Test
  public void testPollAfterSeek()
      throws InterruptedException
  {
    // tests that after doing a seek, the now invalid records in buffer is cleaned up properly

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
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD1_ITERATOR)))
            .andReturn(getRecordsResult1)
            .once();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR)))
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
        0,
        2,
        10_000,
        5000,
        5000,
        1_000_000,
        true,
        false
    );

    recordSupplier.assign(partitions);
    recordSupplier.seek(StreamPartition.of(STREAM, SHARD_ID1), "5");
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 6; i++) {
      Thread.sleep(100);
    }

    OrderedPartitionableRecord<String, String, ByteEntity> firstRecord = recordSupplier.poll(POLL_TIMEOUT_MILLIS).get(0);

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


    OrderedPartitionableRecord<String, String, ByteEntity> record2 = recordSupplier.poll(POLL_TIMEOUT_MILLIS).get(0);

    Assert.assertEquals(ALL_RECORDS.get(9), record2);
    // only one partition in this test. second results come from getRecordsResult0, which has SHARD0_LAG_MILLIS
    Assert.assertEquals(ImmutableMap.of(SHARD_ID1, SHARD0_LAG_MILLIS), recordSupplier.getPartitionResourcesTimeLag());
    verifyAll();
  }


  @Test
  public void testPollDeaggregate() throws InterruptedException
  {
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
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR)))
            .andReturn(getRecordsResult0)
            .anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD1_ITERATOR)))
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
        0,
        2,
        10_000,
        5000,
        5000,
        1_000_000,
        true,
        false
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 12; i++) {
      Thread.sleep(100);
    }

    List<OrderedPartitionableRecord<String, String, ByteEntity>> polledRecords = cleanRecords(recordSupplier.poll(
        POLL_TIMEOUT_MILLIS));

    verifyAll();

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertTrue(polledRecords.containsAll(ALL_RECORDS));
    Assert.assertEquals(SHARDS_LAG_MILLIS, recordSupplier.getPartitionResourcesTimeLag());
  }

  @Test
  public void getLatestSequenceNumberWhenShardIsEmptyShouldReturnUnreadToken()
  {

    KinesisRecordSupplier recordSupplier = getSequenceNumberWhenNoRecordsHelperForOpenShard();
    Assert.assertEquals(KinesisSequenceNumber.UNREAD_LATEST,
                        recordSupplier.getLatestSequenceNumber(StreamPartition.of(STREAM, SHARD_ID0)));
    verifyAll();
  }

  @Test
  public void getEarliestSequenceNumberWhenShardIsEmptyShouldReturnUnreadToken()
  {

    KinesisRecordSupplier recordSupplier = getSequenceNumberWhenNoRecordsHelperForOpenShard();
    Assert.assertEquals(KinesisSequenceNumber.UNREAD_TRIM_HORIZON,
                        recordSupplier.getEarliestSequenceNumber(StreamPartition.of(STREAM, SHARD_ID0)));
    verifyAll();
  }

  @Test
  public void getLatestSequenceNumberWhenKinesisRetryableException()
  {
    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.eq(STREAM),
        EasyMock.eq(SHARD_ID0),
        EasyMock.eq(ShardIteratorType.LATEST.toString())
    )).andReturn(
        getShardIteratorResult0).once();

    EasyMock.expect(getShardIteratorResult0.getShardIterator()).andReturn(SHARD0_ITERATOR).once();

    AmazonClientException ex = new AmazonClientException(new IOException());
    EasyMock.expect(kinesis.getRecords(generateGetRecordsWithLimitReq(SHARD0_ITERATOR, 1000)))
            .andThrow(ex)
            .andReturn(getRecordsResult0)
            .once();

    EasyMock.expect(getRecordsResult0.getRecords()).andReturn(SHARD0_RECORDS).once();
    EasyMock.expect(getRecordsResult0.getNextShardIterator()).andReturn(null).once();
    EasyMock.expect(getRecordsResult0.getMillisBehindLatest()).andReturn(0L).once();

    replayAll();

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        0,
        2,
        10_000,
        5000,
        5000,
        1_000_000,
        true,
        false
    );

    Assert.assertEquals("0", recordSupplier.getLatestSequenceNumber(StreamPartition.of(STREAM, SHARD_ID0)));
  }

  private KinesisRecordSupplier getSequenceNumberWhenNoRecordsHelperForOpenShard()
  {
    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.eq(STREAM),
        EasyMock.eq(SHARD_ID0),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult0).times(1);

    EasyMock.expect(getShardIteratorResult0.getShardIterator()).andReturn(SHARD0_ITERATOR).times(1);

    EasyMock.expect(kinesis.getRecords(generateGetRecordsWithLimitReq(SHARD0_ITERATOR, 1000)))
            .andReturn(getRecordsResult0)
            .times(1);

    EasyMock.expect(getRecordsResult0.getRecords()).andReturn(Collections.emptyList()).times(1);
    EasyMock.expect(getRecordsResult0.getNextShardIterator()).andReturn(SHARD0_ITERATOR).times(1);

    replayAll();

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        0,
        2,
        10_000,
        5000,
        5000,
        1_000_000,
        true,
        false
    );
    return recordSupplier;
  }

  @Test
  public void getPartitionTimeLag() throws InterruptedException
  {
    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID0),
            EasyMock.eq(ShardIteratorType.TRIM_HORIZON.toString()),
            EasyMock.or(EasyMock.matches("\\d+"), EasyMock.isNull())
    )).andReturn(getShardIteratorResult0).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
            EasyMock.anyObject(),
            EasyMock.eq(SHARD_ID0),
            EasyMock.eq(ShardIteratorType.AT_SEQUENCE_NUMBER.toString()),
            EasyMock.matches("\\d+")
    )).andReturn(getShardIteratorResult0).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
            EasyMock.anyObject(),
            EasyMock.eq(SHARD_ID0),
            EasyMock.eq(ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString()),
            EasyMock.matches("\\d+")
    )).andReturn(getShardIteratorResult0).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
            EasyMock.anyObject(),
            EasyMock.eq(SHARD_ID1),
            EasyMock.eq(ShardIteratorType.TRIM_HORIZON.toString()),
            EasyMock.or(EasyMock.matches("\\d+"), EasyMock.isNull())
    )).andReturn(getShardIteratorResult1).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
            EasyMock.anyObject(),
            EasyMock.eq(SHARD_ID1),
            EasyMock.eq(ShardIteratorType.AT_SEQUENCE_NUMBER.toString()),
            EasyMock.matches("\\d+")
    )).andReturn(getShardIteratorResult1).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
            EasyMock.anyObject(),
            EasyMock.eq(SHARD_ID1),
            EasyMock.eq(ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString()),
            EasyMock.matches("\\d+")
    )).andReturn(getShardIteratorResult1).anyTimes();

    EasyMock.expect(getShardIteratorResult0.getShardIterator()).andReturn(SHARD0_ITERATOR).anyTimes();
    EasyMock.expect(getShardIteratorResult1.getShardIterator()).andReturn(SHARD1_ITERATOR).anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR)))
            .andReturn(getRecordsResult0)
            .anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD1_ITERATOR)))
            .andReturn(getRecordsResult1)
            .anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsWithLimitReq(SHARD0_ITERATOR, 1)))
        .andReturn(getRecordsResult0)
        .anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsWithLimitReq(SHARD1_ITERATOR, 1)))
        .andReturn(getRecordsResult1)
        .anyTimes();
    EasyMock.expect(getRecordsResult0.getRecords()).andReturn(SHARD0_RECORDS).times(2);
    EasyMock.expect(getRecordsResult1.getRecords()).andReturn(SHARD1_RECORDS_EMPTY).times(2);
    EasyMock.expect(getRecordsResult0.getNextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult1.getNextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult0.getMillisBehindLatest()).andReturn(SHARD0_LAG_MILLIS).times(2);
    EasyMock.expect(getRecordsResult1.getMillisBehindLatest()).andReturn(SHARD1_LAG_MILLIS_EMPTY).once();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(STREAM, SHARD_ID0),
        StreamPartition.of(STREAM, SHARD_ID1)
    );

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        0,
        2,
        10_000,
        5000,
        5000,
        1_000_000,
        true,
        false
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 12; i++) {
      Thread.sleep(100);
    }

    Map<String, Long> timeLag = recordSupplier.getPartitionResourcesTimeLag();


    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(SHARDS_LAG_MILLIS_EMPTY, timeLag);

    Map<String, String> offsets = ImmutableMap.of(
        SHARD_ID1, SHARD1_RECORDS.get(0).getSequenceNumber(),
        SHARD_ID0, SHARD0_RECORDS.get(0).getSequenceNumber()
    );
    Map<String, Long> independentTimeLag = recordSupplier.getPartitionsTimeLag(STREAM, offsets);
    Assert.assertEquals(SHARDS_LAG_MILLIS_EMPTY, independentTimeLag);

    // Verify that kinesis apis are not called for custom sequence numbers
    for (String sequenceNum : Arrays.asList(NO_END_SEQUENCE_NUMBER, END_OF_SHARD_MARKER, EXPIRED_MARKER,
                                            UNREAD_LATEST, UNREAD_TRIM_HORIZON)) {
      offsets = ImmutableMap.of(
          SHARD_ID1, sequenceNum,
          SHARD_ID0, sequenceNum
      );

      Map<String, Long> zeroOffsets = ImmutableMap.of(
          SHARD_ID1, 0L,
          SHARD_ID0, 0L
      );

      Assert.assertEquals(zeroOffsets, recordSupplier.getPartitionsTimeLag(STREAM, offsets));
    }
    verifyAll();
  }

  @Test
  public void testIsOffsetAvailable()
  {
    AmazonKinesis mockKinesis = EasyMock.mock(AmazonKinesis.class);
    KinesisRecordSupplier target = new KinesisRecordSupplier(
        mockKinesis,
        0,
        2,
        100,
        5000,
        5000,
        1_000_000,
        true,
        false
    );
    StreamPartition<String> partition = new StreamPartition<>(STREAM, SHARD_ID0);

    setupMockKinesisForShardId(mockKinesis, SHARD_ID0, null,
                               ShardIteratorType.AT_SEQUENCE_NUMBER, "-1",
                               Collections.emptyList(), "whatever");

    Record record0 = new Record().withSequenceNumber("5");
    setupMockKinesisForShardId(mockKinesis, SHARD_ID0, null,
                               ShardIteratorType.AT_SEQUENCE_NUMBER, "0",
                               Collections.singletonList(record0), "whatever");

    Record record10 = new Record().withSequenceNumber("10");
    setupMockKinesisForShardId(mockKinesis, SHARD_ID0, null,
                               ShardIteratorType.AT_SEQUENCE_NUMBER, "10",
                               Collections.singletonList(record10), "whatever");

    EasyMock.replay(mockKinesis);

    Assert.assertTrue(target.isOffsetAvailable(partition, KinesisSequenceNumber.of(UNREAD_TRIM_HORIZON)));

    Assert.assertFalse(target.isOffsetAvailable(partition, KinesisSequenceNumber.of(END_OF_SHARD_MARKER)));

    Assert.assertFalse(target.isOffsetAvailable(partition, KinesisSequenceNumber.of("-1")));

    Assert.assertFalse(target.isOffsetAvailable(partition, KinesisSequenceNumber.of("0")));

    Assert.assertTrue(target.isOffsetAvailable(partition, KinesisSequenceNumber.of("10")));
  }

  private void setupMockKinesisForShardId(AmazonKinesis kinesis, String shardId,
                                          List<Record> records, String nextIterator)
  {
    setupMockKinesisForShardId(kinesis, shardId, 1, ShardIteratorType.TRIM_HORIZON, null, records, nextIterator);
  }

  private void setupMockKinesisForShardId(AmazonKinesis kinesis, String shardId, Integer limit,
                                          ShardIteratorType iteratorType, String sequenceNumber,
                                          List<Record> records, String nextIterator)
  {
    String shardIteratorType = iteratorType.toString();
    String shardIterator = "shardIterator" + shardId;
    if (sequenceNumber != null) {
      shardIterator += sequenceNumber;
    }
    GetShardIteratorResult shardIteratorResult = new GetShardIteratorResult().withShardIterator(shardIterator);
    if (sequenceNumber == null) {
      EasyMock.expect(kinesis.getShardIterator(STREAM, shardId, shardIteratorType))
              .andReturn(shardIteratorResult)
              .once();
    } else {
      EasyMock.expect(kinesis.getShardIterator(STREAM, shardId, shardIteratorType, sequenceNumber))
              .andReturn(shardIteratorResult)
              .once();
    }
    GetRecordsRequest request = new GetRecordsRequest().withShardIterator(shardIterator)
                                                       .withLimit(limit);
    GetRecordsResult result = new GetRecordsResult().withRecords(records)
                                                    .withNextShardIterator(nextIterator);
    EasyMock.expect(kinesis.getRecords(request)).andReturn(result);
  }
}
