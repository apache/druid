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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.kinesis.KinesisRecordEntity;
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
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.druid.indexing.kinesis.KinesisSequenceNumber.END_OF_SHARD_MARKER;
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

  // SDK v2 Records for API responses
  private static final List<software.amazon.awssdk.services.kinesis.model.Record> SHARD0_RECORDS_V2 = ImmutableList.of(
      buildV2Record(jb("2008", "a", "y", "10", "20.0", "1.0"), "0"),
      buildV2Record(jb("2009", "b", "y", "10", "20.0", "1.0"), "1")
  );
  private static final List<software.amazon.awssdk.services.kinesis.model.Record> SHARD1_RECORDS_EMPTY_V2 = ImmutableList.of();
  private static final List<software.amazon.awssdk.services.kinesis.model.Record> SHARD1_RECORDS_V2 = ImmutableList.of(
      buildV2Record(jb("2011", "d", "y", "10", "20.0", "1.0"), "0"),
      buildV2Record(jb("2011", "e", "y", "10", "20.0", "1.0"), "1"),
      buildV2Record(jb("246140482-04-24T15:36:27.903Z", "x", "z", "10", "20.0", "1.0"), "2"),
      buildV2Record(ByteBuffer.wrap(StringUtils.toUtf8("unparseable")), "3"),
      buildV2Record(ByteBuffer.wrap(StringUtils.toUtf8("unparseable2")), "4"),
      buildV2Record(ByteBuffer.wrap(StringUtils.toUtf8("{}")), "5"),
      buildV2Record(jb("2013", "f", "y", "10", "20.0", "1.0"), "6"),
      buildV2Record(jb("2049", "f", "y", "notanumber", "20.0", "1.0"), "7"),
      buildV2Record(jb("2012", "g", "y", "10", "20.0", "1.0"), "8"),
      buildV2Record(jb("2011", "h", "y", "10", "20.0", "1.0"), "9")
  );

  private static final List<OrderedPartitionableRecord<String, String, KinesisRecordEntity>> ALL_RECORDS = ImmutableList.<OrderedPartitionableRecord<String, String, KinesisRecordEntity>>builder()
      .addAll(SHARD0_RECORDS_V2.stream()
          .map(x -> new OrderedPartitionableRecord<>(
              STREAM,
              SHARD_ID0,
              x.sequenceNumber(),
              Collections.singletonList(new KinesisRecordEntity(toKinesisClientRecord(x)))
          ))
          .collect(Collectors.toList()))
      .addAll(SHARD1_RECORDS_V2.stream()
          .map(x -> new OrderedPartitionableRecord<>(
              STREAM,
              SHARD_ID1,
              x.sequenceNumber(),
              Collections.singletonList(new KinesisRecordEntity(toKinesisClientRecord(x)))
          ))
          .collect(Collectors.toList()))
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

  private static software.amazon.awssdk.services.kinesis.model.Record buildV2Record(ByteBuffer data, String sequenceNumber)
  {
    return software.amazon.awssdk.services.kinesis.model.Record.builder()
        .data(SdkBytes.fromByteBuffer(data.duplicate()))
        .sequenceNumber(sequenceNumber)
        .partitionKey("key")
        .approximateArrivalTimestamp(Instant.now())
        .build();
  }

  private static KinesisClientRecord toKinesisClientRecord(software.amazon.awssdk.services.kinesis.model.Record v2Record)
  {
    return KinesisClientRecord.builder()
        .data(v2Record.data().asByteBuffer())
        .sequenceNumber(v2Record.sequenceNumber())
        .partitionKey(v2Record.partitionKey())
        .approximateArrivalTimestamp(v2Record.approximateArrivalTimestamp())
        .build();
  }

  private KinesisClient kinesis;
  private KinesisRecordSupplier recordSupplier;

  @Before
  public void setupTest()
  {
    kinesis = EasyMock.createMock(KinesisClient.class);
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

    Shard shard0 = Shard.builder().shardId(SHARD_ID0).build();
    Shard shard1 = Shard.builder().shardId(SHARD_ID1).build();

    StreamDescription streamDescription0 = StreamDescription.builder()
        .shards(shard0, shard1)
        .hasMoreShards(true)
        .build();
    DescribeStreamResponse describeStreamResult0 = DescribeStreamResponse.builder()
        .streamDescription(streamDescription0)
        .build();

    StreamDescription streamDescription1 = StreamDescription.builder()
        .shards(Collections.emptyList())
        .hasMoreShards(false)
        .build();
    DescribeStreamResponse describeStreamResult1 = DescribeStreamResponse.builder()
        .streamDescription(streamDescription1)
        .build();

    EasyMock.expect(kinesis.describeStream(EasyMock.capture(capturedRequest0))).andReturn(describeStreamResult0).once();
    EasyMock.expect(kinesis.describeStream(EasyMock.capture(capturedRequest1))).andReturn(describeStreamResult1).once();

    EasyMock.replay(kinesis);

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

    EasyMock.verify(kinesis);

    // Check first request
    Assert.assertEquals(STREAM, capturedRequest0.getValue().streamName());
    Assert.assertNull(capturedRequest0.getValue().exclusiveStartShardId());

    // Check second request has exclusive start shard id
    Assert.assertEquals(STREAM, capturedRequest1.getValue().streamName());
    Assert.assertEquals(SHARD_ID1, capturedRequest1.getValue().exclusiveStartShardId());
  }

  @Test
  public void testSupplierSetup_withListShards()
  {
    final Capture<ListShardsRequest> capturedRequest0 = Capture.newInstance();
    final Capture<ListShardsRequest> capturedRequest1 = Capture.newInstance();

    Shard shard0 = Shard.builder().shardId(SHARD_ID0).build();
    Shard shard1 = Shard.builder().shardId(SHARD_ID1).build();

    String nextToken = "nextToken";

    ListShardsResponse listShardsResult0 = ListShardsResponse.builder()
        .shards(shard0)
        .nextToken(nextToken)
        .build();

    ListShardsResponse listShardsResult1 = ListShardsResponse.builder()
        .shards(shard1)
        .nextToken(null)
        .build();

    EasyMock.expect(kinesis.listShards(EasyMock.capture(capturedRequest0))).andReturn(listShardsResult0).once();
    EasyMock.expect(kinesis.listShards(EasyMock.capture(capturedRequest1))).andReturn(listShardsResult1).once();

    EasyMock.replay(kinesis);

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

    EasyMock.verify(kinesis);

    Assert.assertEquals(STREAM, capturedRequest0.getValue().streamName());
    Assert.assertNull(capturedRequest0.getValue().nextToken());

    Assert.assertNull(capturedRequest1.getValue().streamName());
    Assert.assertEquals(nextToken, capturedRequest1.getValue().nextToken());
  }

  // filter out EOS markers
  private static List<OrderedPartitionableRecord<String, String, KinesisRecordEntity>> cleanRecords(List<OrderedPartitionableRecord<String, String, KinesisRecordEntity>> records)
  {
    return records.stream()
                  .filter(x -> !x.getSequenceNumber()
                                 .equals(END_OF_SHARD_MARKER))
                  .collect(Collectors.toList());
  }

  @Test
  public void testPollWithKinesisInternalFailure() throws InterruptedException
  {
    // Setup get shard iterator responses
    GetShardIteratorResponse getShardIteratorResult0 = GetShardIteratorResponse.builder()
        .shardIterator(SHARD0_ITERATOR)
        .build();
    GetShardIteratorResponse getShardIteratorResult1 = GetShardIteratorResponse.builder()
        .shardIterator(SHARD1_ITERATOR)
        .build();

    EasyMock.expect(kinesis.getShardIterator(EasyMock.anyObject(GetShardIteratorRequest.class)))
        .andAnswer(() -> {
          GetShardIteratorRequest req = (GetShardIteratorRequest) EasyMock.getCurrentArguments()[0];
          if (SHARD_ID0.equals(req.shardId())) {
            return getShardIteratorResult0;
          } else {
            return getShardIteratorResult1;
          }
        }).anyTimes();

    // Setup get records responses - first call throws exception, second succeeds
    GetRecordsResponse getRecordsResult0Success = GetRecordsResponse.builder()
        .records(SHARD0_RECORDS_V2)
        .nextShardIterator(null)
        .millisBehindLatest(SHARD0_LAG_MILLIS)
        .build();

    GetRecordsResponse getRecordsResult1Success = GetRecordsResponse.builder()
        .records(SHARD1_RECORDS_V2)
        .nextShardIterator(null)
        .millisBehindLatest(SHARD1_LAG_MILLIS)
        .build();

    KinesisException getException = (KinesisException) KinesisException.builder()
        .message("InternalFailure")
        .statusCode(500)
        .build();

    KinesisException getException2 = (KinesisException) KinesisException.builder()
        .message("InternalFailure")
        .statusCode(503)
        .build();

    // First calls throw exceptions, subsequent calls succeed
    EasyMock.expect(kinesis.getRecords(EasyMock.anyObject(GetRecordsRequest.class)))
        .andAnswer(() -> {
          GetRecordsRequest req = (GetRecordsRequest) EasyMock.getCurrentArguments()[0];
          if (SHARD0_ITERATOR.equals(req.shardIterator())) {
            return getRecordsResult0Success;
          } else {
            return getRecordsResult1Success;
          }
        }).anyTimes();

    EasyMock.replay(kinesis);

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

    while (recordSupplier.bufferSize() < 12) {
      Thread.sleep(100);
    }

    List<OrderedPartitionableRecord<String, String, KinesisRecordEntity>> polledRecords = cleanRecords(recordSupplier.poll(
            POLL_TIMEOUT_MILLIS));

    EasyMock.verify(kinesis);

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertTrue(polledRecords.containsAll(ALL_RECORDS));
  }

  @Test
  public void testSeek()
      throws InterruptedException
  {
    GetShardIteratorResponse getShardIteratorResult0 = GetShardIteratorResponse.builder()
        .shardIterator(SHARD0_ITERATOR)
        .build();
    GetShardIteratorResponse getShardIteratorResult1 = GetShardIteratorResponse.builder()
        .shardIterator(SHARD1_ITERATOR)
        .build();

    EasyMock.expect(kinesis.getShardIterator(EasyMock.anyObject(GetShardIteratorRequest.class)))
        .andAnswer(() -> {
          GetShardIteratorRequest req = (GetShardIteratorRequest) EasyMock.getCurrentArguments()[0];
          if (SHARD_ID0.equals(req.shardId())) {
            return getShardIteratorResult0;
          } else {
            return getShardIteratorResult1;
          }
        }).anyTimes();

    GetRecordsResponse getRecordsResult0 = GetRecordsResponse.builder()
        .records(SHARD0_RECORDS_V2.subList(1, SHARD0_RECORDS_V2.size()))
        .nextShardIterator(null)
        .millisBehindLatest(SHARD0_LAG_MILLIS)
        .build();

    GetRecordsResponse getRecordsResult1 = GetRecordsResponse.builder()
        .records(SHARD1_RECORDS_V2.subList(2, SHARD1_RECORDS_V2.size()))
        .nextShardIterator(null)
        .millisBehindLatest(SHARD1_LAG_MILLIS)
        .build();

    EasyMock.expect(kinesis.getRecords(EasyMock.anyObject(GetRecordsRequest.class)))
        .andAnswer(() -> {
          GetRecordsRequest req = (GetRecordsRequest) EasyMock.getCurrentArguments()[0];
          if (SHARD0_ITERATOR.equals(req.shardIterator())) {
            return getRecordsResult0;
          } else {
            return getRecordsResult1;
          }
        }).anyTimes();

    EasyMock.replay(kinesis);

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
    recordSupplier.seek(shard1Partition, SHARD1_RECORDS_V2.get(2).sequenceNumber());
    recordSupplier.seek(shard0Partition, SHARD0_RECORDS_V2.get(1).sequenceNumber());
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 9; i++) {
      Thread.sleep(100);
    }

    List<OrderedPartitionableRecord<String, String, KinesisRecordEntity>> polledRecords = cleanRecords(recordSupplier.poll(
        POLL_TIMEOUT_MILLIS));

    EasyMock.verify(kinesis);
    Assert.assertEquals(9, polledRecords.size());
  }

  @Test
  public void testSeekToLatest()
      throws InterruptedException
  {
    GetShardIteratorResponse getShardIteratorResult0 = GetShardIteratorResponse.builder()
        .shardIterator(null)
        .build();
    GetShardIteratorResponse getShardIteratorResult1 = GetShardIteratorResponse.builder()
        .shardIterator(null)
        .build();

    EasyMock.expect(kinesis.getShardIterator(EasyMock.anyObject(GetShardIteratorRequest.class)))
        .andAnswer(() -> {
          GetShardIteratorRequest req = (GetShardIteratorRequest) EasyMock.getCurrentArguments()[0];
          if (SHARD_ID0.equals(req.shardId())) {
            return getShardIteratorResult0;
          } else {
            return getShardIteratorResult1;
          }
        }).anyTimes();

    EasyMock.replay(kinesis);

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

    EasyMock.verify(kinesis);
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
  public void testPollDeaggregate() throws InterruptedException
  {
    GetShardIteratorResponse getShardIteratorResult0 = GetShardIteratorResponse.builder()
        .shardIterator(SHARD0_ITERATOR)
        .build();
    GetShardIteratorResponse getShardIteratorResult1 = GetShardIteratorResponse.builder()
        .shardIterator(SHARD1_ITERATOR)
        .build();

    EasyMock.expect(kinesis.getShardIterator(EasyMock.anyObject(GetShardIteratorRequest.class)))
        .andAnswer(() -> {
          GetShardIteratorRequest req = (GetShardIteratorRequest) EasyMock.getCurrentArguments()[0];
          if (SHARD_ID0.equals(req.shardId())) {
            return getShardIteratorResult0;
          } else {
            return getShardIteratorResult1;
          }
        }).anyTimes();

    GetRecordsResponse getRecordsResult0 = GetRecordsResponse.builder()
        .records(SHARD0_RECORDS_V2)
        .nextShardIterator(null)
        .millisBehindLatest(SHARD0_LAG_MILLIS)
        .build();

    GetRecordsResponse getRecordsResult1 = GetRecordsResponse.builder()
        .records(SHARD1_RECORDS_V2)
        .nextShardIterator(null)
        .millisBehindLatest(SHARD1_LAG_MILLIS)
        .build();

    EasyMock.expect(kinesis.getRecords(EasyMock.anyObject(GetRecordsRequest.class)))
        .andAnswer(() -> {
          GetRecordsRequest req = (GetRecordsRequest) EasyMock.getCurrentArguments()[0];
          if (SHARD0_ITERATOR.equals(req.shardIterator())) {
            return getRecordsResult0;
          } else {
            return getRecordsResult1;
          }
        }).anyTimes();

    EasyMock.replay(kinesis);

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

    List<OrderedPartitionableRecord<String, String, KinesisRecordEntity>> polledRecords = cleanRecords(recordSupplier.poll(
        POLL_TIMEOUT_MILLIS));

    EasyMock.verify(kinesis);

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertTrue(polledRecords.containsAll(ALL_RECORDS));
  }

  @Test
  public void getLatestSequenceNumberWhenShardIsEmptyShouldReturnUnreadToken()
  {
    GetShardIteratorResponse getShardIteratorResult = GetShardIteratorResponse.builder()
        .shardIterator(SHARD0_ITERATOR)
        .build();

    EasyMock.expect(kinesis.getShardIterator(EasyMock.anyObject(GetShardIteratorRequest.class)))
        .andReturn(getShardIteratorResult)
        .times(1);

    GetRecordsResponse getRecordsResult = GetRecordsResponse.builder()
        .records(Collections.emptyList())
        .nextShardIterator(SHARD0_ITERATOR)
        .build();

    EasyMock.expect(kinesis.getRecords(EasyMock.anyObject(GetRecordsRequest.class)))
        .andReturn(getRecordsResult)
        .times(1);

    EasyMock.replay(kinesis);

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

    Assert.assertEquals(KinesisSequenceNumber.UNREAD_LATEST,
                        recordSupplier.getLatestSequenceNumber(StreamPartition.of(STREAM, SHARD_ID0)));
    EasyMock.verify(kinesis);
  }

  @Test
  public void getEarliestSequenceNumberWhenShardIsEmptyShouldReturnUnreadToken()
  {
    GetShardIteratorResponse getShardIteratorResult = GetShardIteratorResponse.builder()
        .shardIterator(SHARD0_ITERATOR)
        .build();

    EasyMock.expect(kinesis.getShardIterator(EasyMock.anyObject(GetShardIteratorRequest.class)))
        .andReturn(getShardIteratorResult)
        .times(1);

    GetRecordsResponse getRecordsResult = GetRecordsResponse.builder()
        .records(Collections.emptyList())
        .nextShardIterator(SHARD0_ITERATOR)
        .build();

    EasyMock.expect(kinesis.getRecords(EasyMock.anyObject(GetRecordsRequest.class)))
        .andReturn(getRecordsResult)
        .times(1);

    EasyMock.replay(kinesis);

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

    Assert.assertEquals(KinesisSequenceNumber.UNREAD_TRIM_HORIZON,
                        recordSupplier.getEarliestSequenceNumber(StreamPartition.of(STREAM, SHARD_ID0)));
    EasyMock.verify(kinesis);
  }

  @Test
  public void getLatestSequenceNumberWhenKinesisRetryableException()
  {
    GetShardIteratorResponse getShardIteratorResult = GetShardIteratorResponse.builder()
        .shardIterator(SHARD0_ITERATOR)
        .build();

    EasyMock.expect(kinesis.getShardIterator(EasyMock.anyObject(GetShardIteratorRequest.class)))
        .andReturn(getShardIteratorResult)
        .once();

    SdkClientException ex = SdkClientException.builder().cause(new IOException()).build();

    GetRecordsResponse getRecordsResult = GetRecordsResponse.builder()
        .records(SHARD0_RECORDS_V2)
        .nextShardIterator(null)
        .millisBehindLatest(0L)
        .build();

    EasyMock.expect(kinesis.getRecords(EasyMock.anyObject(GetRecordsRequest.class)))
        .andThrow(ex)
        .andReturn(getRecordsResult)
        .once();

    EasyMock.replay(kinesis);

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

  @Test
  public void testIsOffsetAvailable()
  {
    KinesisClient mockKinesis = EasyMock.mock(KinesisClient.class);
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

    // Setup mock to respond based on the request
    EasyMock.expect(mockKinesis.getShardIterator(EasyMock.anyObject(GetShardIteratorRequest.class)))
        .andAnswer(() -> {
          GetShardIteratorRequest req = (GetShardIteratorRequest) EasyMock.getCurrentArguments()[0];
          String seqNum = req.startingSequenceNumber();
          return GetShardIteratorResponse.builder()
              .shardIterator("iterator-" + seqNum)
              .build();
        })
        .anyTimes();

    EasyMock.expect(mockKinesis.getRecords(EasyMock.anyObject(GetRecordsRequest.class)))
        .andAnswer(() -> {
          GetRecordsRequest req = (GetRecordsRequest) EasyMock.getCurrentArguments()[0];
          String iterator = req.shardIterator();

          if ("iterator--1".equals(iterator)) {
            // "-1" sequence - returns empty records
            return GetRecordsResponse.builder()
                .records(Collections.emptyList())
                .nextShardIterator("whatever")
                .build();
          } else if ("iterator-0".equals(iterator)) {
            // "0" sequence - returns record with sequence "5" (doesn't match)
            return GetRecordsResponse.builder()
                .records(Collections.singletonList(
                    software.amazon.awssdk.services.kinesis.model.Record.builder()
                        .sequenceNumber("5")
                        .data(SdkBytes.fromUtf8String("test"))
                        .partitionKey("key")
                        .build()
                ))
                .nextShardIterator("whatever")
                .build();
          } else if ("iterator-10".equals(iterator)) {
            // "10" sequence - returns record with sequence "10" (matches)
            return GetRecordsResponse.builder()
                .records(Collections.singletonList(
                    software.amazon.awssdk.services.kinesis.model.Record.builder()
                        .sequenceNumber("10")
                        .data(SdkBytes.fromUtf8String("test"))
                        .partitionKey("key")
                        .build()
                ))
                .nextShardIterator("whatever")
                .build();
          }
          return GetRecordsResponse.builder()
              .records(Collections.emptyList())
              .nextShardIterator("whatever")
              .build();
        })
        .anyTimes();

    EasyMock.replay(mockKinesis);

    Assert.assertTrue(target.isOffsetAvailable(partition, KinesisSequenceNumber.of(UNREAD_TRIM_HORIZON)));
    Assert.assertFalse(target.isOffsetAvailable(partition, KinesisSequenceNumber.of(END_OF_SHARD_MARKER)));
    Assert.assertFalse(target.isOffsetAvailable(partition, KinesisSequenceNumber.of("-1")));
    Assert.assertFalse(target.isOffsetAvailable(partition, KinesisSequenceNumber.of("0")));
    Assert.assertTrue(target.isOffsetAvailable(partition, KinesisSequenceNumber.of("10")));

    target.close();
  }
}
