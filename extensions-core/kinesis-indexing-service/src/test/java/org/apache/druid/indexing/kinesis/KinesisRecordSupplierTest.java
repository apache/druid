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

import cloud.localstack.LocalstackTestRunner;
import cloud.localstack.TestUtils;
import cloud.localstack.docker.LocalstackDockerTestRunner;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import com.amazonaws.http.SdkHttpMetadata;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(LocalstackDockerTestRunner.class)
@LocalstackDockerProperties(services = {"kinesis"})
public class KinesisRecordSupplierTest
{
  static {
    TestUtils.setEnv("AWS_CBOR_DISABLE", "1");
  }

  private static final Logger log = new Logger(KinesisRecordSupplierTest.class);
  private static String stream = "streamm";
  private static long poll_timeout_millis = 2000;
  private static String shardId1 = "shardId-000000000001";
  private static String shardId0 = "shardId-000000000000";
  private static int streamPosFix = 0;
  private static int pollRetry = 10;
  private static KinesisRecordSupplier recordSupplier;
  private static final List<PutRecordsRequestEntry> records = ImmutableList.of(
      generateRequestEntry(
          "1",
          JB("2011", "d", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("2011", "e", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("246140482-04-24T15:36:27.903Z", "x", "z", "10", "20.0", "1.0")
      ),
      generateRequestEntry("1", StringUtils.toUtf8("unparseable")),
      generateRequestEntry(
          "1",
          StringUtils.toUtf8("unparseable2")
      ),
      generateRequestEntry("1", StringUtils.toUtf8("{}")),
      generateRequestEntry(
          "1",
          JB("2013", "f", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "1",
          JB("2049", "f", "y", "notanumber", "20.0", "1.0")
      ),
      generateRequestEntry(
          "123123",
          JB("2012", "g", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "123123",
          JB("2011", "h", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "123123",
          JB("2008", "a", "y", "10", "20.0", "1.0")
      ),
      generateRequestEntry(
          "123123",
          JB("2009", "b", "y", "10", "20.0", "1.0")
      )
  );

  private static PutRecordsRequestEntry generateRequestEntry(String partition, byte[] data)
  {
    return new PutRecordsRequestEntry().withPartitionKey(partition)
                                       .withData(ByteBuffer.wrap(data));
  }

  private static byte[] JB(String timestamp, String dim1, String dim2, String dimLong, String dimFloat, String met1)
  {
    try {
      return new ObjectMapper().writeValueAsBytes(
          ImmutableMap.builder()
                      .put("timestamp", timestamp)
                      .put("dim1", dim1)
                      .put("dim2", dim2)
                      .put("dimLong", dimLong)
                      .put("dimFloat", dimFloat)
                      .put("met1", met1)
                      .build()
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }


  private AmazonKinesis getKinesisClientInstance() throws InterruptedException
  {
    AmazonKinesis kinesis = TestUtils.getClientKinesis();
    SdkHttpMetadata createRes = kinesis.createStream(stream, 2).getSdkHttpMetadata();
    // sleep required because of kinesalite
    Thread.sleep(500);
    return kinesis;
  }

  private static PutRecordsRequest generateRecordsRequests(String stream)
  {
    return new PutRecordsRequest()
        .withStreamName(stream)
        .withRecords(records);
  }

  private static PutRecordsRequest generateRecordsRequests(String stream, int first, int last)
  {
    return new PutRecordsRequest()
        .withStreamName(stream)
        .withRecords(records.subList(first, last));
  }

  private static List<PutRecordsResultEntry> insertData(
      AmazonKinesis kinesis,
      PutRecordsRequest req
  )
  {
    PutRecordsResult res = kinesis.putRecords(req);
    Assert.assertEquals((int) res.getFailedRecordCount(), 0);
    return res.getRecords();
  }

  private static String getStreamName()
  {
    return "stream-" + streamPosFix++;
  }

  @Before
  public void setupTest()
  {
    stream = getStreamName();
  }

  @After
  public void tearDownTest()
  {
    recordSupplier.close();
    recordSupplier = null;
  }

  @Test
  public void testSupplierSetup() throws InterruptedException
  {

    getKinesisClientInstance();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(stream, shardId0),
        StreamPartition.of(stream, shardId1)
    );

    recordSupplier = new KinesisRecordSupplier(
        LocalstackTestRunner.getEndpointKinesis(),
        TestUtils.TEST_ACCESS_KEY,
        TestUtils.TEST_SECRET_KEY,
        1,
        0,
        2,
        null,
        null,
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
  }

  @Test
  public void testPoll() throws InterruptedException
  {
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> insertDataResults = insertData(kinesis, generateRecordsRequests(stream));
    Set<OrderedPartitionableRecord<String, String>> initialRecords = insertDataResults.stream()
                                                                                      .map(r -> new OrderedPartitionableRecord<>(
                                                                                          stream,
                                                                                          r.getShardId(),
                                                                                          r.getSequenceNumber(),
                                                                                          null
                                                                                      ))
                                                                                      .collect(Collectors.toSet());

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(stream, shardId0),
        StreamPartition.of(stream, shardId1)
    );

    recordSupplier = new KinesisRecordSupplier(
        LocalstackTestRunner.getEndpointKinesis(),
        TestUtils.TEST_ACCESS_KEY,
        TestUtils.TEST_SECRET_KEY,
        100,
        0,
        2,
        null,
        null,
        false,
        100,
        5000,
        5000,
        60000,
        100
    );
    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    List<OrderedPartitionableRecord<String, String>> polledRecords = recordSupplier.poll(poll_timeout_millis);
    for (int i = 0; polledRecords.size() != initialRecords.size() && i < pollRetry; i++) {
      polledRecords.addAll(recordSupplier.poll(poll_timeout_millis));
      Thread.sleep(200);
    }

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(initialRecords.size(), polledRecords.size());
    Assert.assertTrue(polledRecords.containsAll(initialRecords));
  }

  @Test
  public void testPollAfterMoreDataAdded() throws InterruptedException
  {
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> insertDataResults1 = insertData(kinesis, generateRecordsRequests(stream, 0, 5));
    Set<OrderedPartitionableRecord<String, String>> initialRecords = insertDataResults1.stream()
                                                                                       .map(r -> new OrderedPartitionableRecord<>(
                                                                                           stream,
                                                                                           r.getShardId(),
                                                                                           r.getSequenceNumber(),
                                                                                           null
                                                                                       ))
                                                                                       .collect(Collectors.toSet());

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(stream, shardId0),
        StreamPartition.of(stream, shardId1)
    );

    recordSupplier = new KinesisRecordSupplier(
        LocalstackTestRunner.getEndpointKinesis(),
        TestUtils.TEST_ACCESS_KEY,
        TestUtils.TEST_SECRET_KEY,
        1,
        0,
        2,
        null,
        null,
        false,
        100,
        5000,
        5000,
        60000,
        5
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    List<OrderedPartitionableRecord<String, String>> polledRecords = recordSupplier.poll(poll_timeout_millis);
    for (int i = 0; polledRecords.size() != initialRecords.size() && i < pollRetry; i++) {
      polledRecords.addAll(recordSupplier.poll(poll_timeout_millis));
      Thread.sleep(200);
    }

    List<PutRecordsResultEntry> insertDataResults2 = insertData(kinesis, generateRecordsRequests(stream, 5, 12));
    insertDataResults2.forEach(entry -> initialRecords.add(new OrderedPartitionableRecord<>(
        stream,
        entry.getShardId(),
        entry.getSequenceNumber(),
        null
    )));

    for (int i = 0; polledRecords.size() != initialRecords.size() && i < pollRetry; i++) {
      polledRecords.addAll(recordSupplier.poll(poll_timeout_millis));
      Thread.sleep(200);
    }

    Assert.assertEquals(initialRecords.size(), polledRecords.size());
    Assert.assertTrue(polledRecords.containsAll(initialRecords));
  }

  @Test
  public void testSeek() throws InterruptedException
  {
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> insertDataResults = insertData(kinesis, generateRecordsRequests(stream));

    StreamPartition<String> shard0 = StreamPartition.of(stream, shardId0);
    StreamPartition<String> shard1 = StreamPartition.of(stream, shardId1);
    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        shard0,
        shard1
    );

    recordSupplier = new KinesisRecordSupplier(
        LocalstackTestRunner.getEndpointKinesis(),
        TestUtils.TEST_ACCESS_KEY,
        TestUtils.TEST_SECRET_KEY,
        1,
        0,
        2,
        null,
        null,
        false,
        100,
        5000,
        5000,
        60000,
        5
    );

    recordSupplier.assign(partitions);

    Assert.assertEquals(insertDataResults.get(0).getSequenceNumber(), recordSupplier.getEarliestSequenceNumber(shard1));
    Assert.assertEquals(insertDataResults.get(8).getSequenceNumber(), recordSupplier.getEarliestSequenceNumber(shard0));

    recordSupplier.seek(shard1, insertDataResults.get(2).getSequenceNumber());
    recordSupplier.seek(shard0, insertDataResults.get(10).getSequenceNumber());

    Set<OrderedPartitionableRecord<String, String>> initialRecords1 = insertDataResults.subList(2, 8).stream()
                                                                                       .map(r -> new OrderedPartitionableRecord<>(
                                                                                           stream,
                                                                                           r.getShardId(),
                                                                                           r.getSequenceNumber(),
                                                                                           null
                                                                                       ))
                                                                                       .collect(Collectors.toSet());

    Set<OrderedPartitionableRecord<String, String>> initialRecords2 = insertDataResults.subList(10, 12).stream()
                                                                                       .map(r -> new OrderedPartitionableRecord<>(
                                                                                           stream,
                                                                                           r.getShardId(),
                                                                                           r.getSequenceNumber(),
                                                                                           null
                                                                                       ))
                                                                                       .collect(Collectors.toSet());

    List<OrderedPartitionableRecord<String, String>> polledRecords = recordSupplier.poll(poll_timeout_millis);
    for (int i = 0; polledRecords.size() != 8 && i < pollRetry; i++) {
      polledRecords.addAll(recordSupplier.poll(poll_timeout_millis));
      Thread.sleep(200);
    }

    Assert.assertEquals(8, polledRecords.size());
    Assert.assertTrue(polledRecords.containsAll(initialRecords1));
    Assert.assertTrue(polledRecords.containsAll(initialRecords2));

  }

  @Test
  public void testSeekToLatest() throws InterruptedException
  {
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> insertDataResults = insertData(kinesis, generateRecordsRequests(stream));

    StreamPartition<String> shard0 = StreamPartition.of(stream, shardId0);
    StreamPartition<String> shard1 = StreamPartition.of(stream, shardId1);
    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        shard0,
        shard1
    );

    recordSupplier = new KinesisRecordSupplier(
        LocalstackTestRunner.getEndpointKinesis(),
        TestUtils.TEST_ACCESS_KEY,
        TestUtils.TEST_SECRET_KEY,
        1,
        0,
        2,
        null,
        null,
        false,
        100,
        5000,
        5000,
        60000,
        5
    );

    recordSupplier.assign(partitions);

    Assert.assertEquals(insertDataResults.get(0).getSequenceNumber(), recordSupplier.getEarliestSequenceNumber(shard1));
    Assert.assertEquals(insertDataResults.get(8).getSequenceNumber(), recordSupplier.getEarliestSequenceNumber(shard0));

    recordSupplier.seekToLatest(partitions);
    Assert.assertEquals(Collections.emptyList(), recordSupplier.poll(poll_timeout_millis));
  }

  @Test(expected = ISE.class)
  public void testSeekUnassigned() throws InterruptedException
  {
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> insertDataResults = insertData(kinesis, generateRecordsRequests(stream));

    StreamPartition<String> shard0 = StreamPartition.of(stream, shardId0);
    StreamPartition<String> shard1 = StreamPartition.of(stream, shardId1);
    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        shard1
    );

    recordSupplier = new KinesisRecordSupplier(
        LocalstackTestRunner.getEndpointKinesis(),
        TestUtils.TEST_ACCESS_KEY,
        TestUtils.TEST_SECRET_KEY,
        1,
        0,
        2,
        null,
        null,
        false,
        100,
        5000,
        5000,
        60000,
        5
    );

    recordSupplier.assign(partitions);

    Assert.assertEquals(insertDataResults.get(0).getSequenceNumber(), recordSupplier.getEarliestSequenceNumber(shard1));

    recordSupplier.seekToEarliest(Collections.singleton(shard0));
  }

  @Test
  public void testPollAfterSeek() throws InterruptedException
  {
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> insertDataResults = insertData(kinesis, generateRecordsRequests(stream));

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(stream, shardId1)
    );

    recordSupplier = new KinesisRecordSupplier(
        LocalstackTestRunner.getEndpointKinesis(),
        TestUtils.TEST_ACCESS_KEY,
        TestUtils.TEST_SECRET_KEY,
        10,
        0,
        2,
        null,
        null,
        false,
        100,
        5000,
        5000,
        60000,
        1
    );
    recordSupplier.assign(partitions);
    recordSupplier.seek(StreamPartition.of(stream, shardId1), getSequenceNumber(insertDataResults, shardId1, 5));

    for (int i = 0; recordSupplier.bufferSize() < 2 && i < pollRetry; i++) {
      Thread.sleep(200);
    }
    OrderedPartitionableRecord<String, String> firstRecord = recordSupplier.poll(poll_timeout_millis).get(0);

    Assert.assertEquals(
        getSequenceNumber(insertDataResults, shardId1, 5),
        firstRecord.getSequenceNumber()
    );

    recordSupplier.seek(StreamPartition.of(stream, shardId1), getSequenceNumber(insertDataResults, shardId1, 7));
    for (int i = 0; recordSupplier.bufferSize() < 2 && i < pollRetry; i++) {
      Thread.sleep(200);
    }

    OrderedPartitionableRecord<String, String> record2 = recordSupplier.poll(poll_timeout_millis).get(0);

    Assert.assertNotNull(record2);
    Assert.assertEquals(stream, record2.getStream());
    Assert.assertEquals(shardId1, record2.getPartitionId());
    Assert.assertEquals(getSequenceNumber(insertDataResults, shardId1, 7), record2.getSequenceNumber());

    recordSupplier.seek(StreamPartition.of(stream, shardId1), getSequenceNumber(insertDataResults, shardId1, 2));
    for (int i = 0; recordSupplier.bufferSize() < 2 && i < pollRetry; i++) {
      Thread.sleep(200);
    }
    OrderedPartitionableRecord<String, String> record3 = recordSupplier.poll(poll_timeout_millis).get(0);

    Assert.assertNotNull(record3);
    Assert.assertEquals(stream, record3.getStream());
    Assert.assertEquals(shardId1, record3.getPartitionId());
    Assert.assertEquals(getSequenceNumber(insertDataResults, shardId1, 2), record3.getSequenceNumber());
  }

  @Test
  public void testPosition() throws InterruptedException
  {
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> insertDataResults = insertData(kinesis, generateRecordsRequests(stream));

    StreamPartition<String> partition1 = StreamPartition.of(stream, shardId1);
    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        partition1
    );

    recordSupplier = new KinesisRecordSupplier(
        LocalstackTestRunner.getEndpointKinesis(),
        TestUtils.TEST_ACCESS_KEY,
        TestUtils.TEST_SECRET_KEY,
        100,
        0,
        2,
        null,
        null,
        false,
        100,
        5000,
        5000,
        60000,
        1
    );
    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    Assert.assertEquals(getSequenceNumber(insertDataResults, shardId1, 0), recordSupplier.getPosition(partition1));

    recordSupplier.seek(partition1, getSequenceNumber(insertDataResults, shardId1, 3));
    Assert.assertEquals(getSequenceNumber(insertDataResults, shardId1, 3), recordSupplier.getPosition(partition1));

    Assert.assertEquals(new OrderedPartitionableRecord<>(
        stream,
        shardId1,
        getSequenceNumber(insertDataResults, shardId1, 3),
        null
    ), recordSupplier.poll(poll_timeout_millis).get(0));

    Assert.assertEquals(getSequenceNumber(insertDataResults, shardId1, 4), recordSupplier.getPosition(partition1));

    Assert.assertEquals(
        getSequenceNumber(insertDataResults, shardId1, 4),
        recordSupplier.poll(poll_timeout_millis).get(0).getSequenceNumber()
    );

    Assert.assertEquals(
        getSequenceNumber(insertDataResults, shardId1, 5),
        recordSupplier.poll(poll_timeout_millis).get(0).getSequenceNumber()
    );

    Assert.assertEquals(
        getSequenceNumber(insertDataResults, shardId1, 6),
        recordSupplier.poll(poll_timeout_millis).get(0).getSequenceNumber()
    );

    Assert.assertEquals(getSequenceNumber(insertDataResults, shardId1, 7), recordSupplier.getPosition(partition1));
  }

  @Test
  public void testPositionAfterPollBatch() throws InterruptedException
  {
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> insertDataResults = insertData(kinesis, generateRecordsRequests(stream));

    StreamPartition<String> partition1 = StreamPartition.of(stream, shardId1);
    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        partition1
    );

    recordSupplier = new KinesisRecordSupplier(
        LocalstackTestRunner.getEndpointKinesis(),
        TestUtils.TEST_ACCESS_KEY,
        TestUtils.TEST_SECRET_KEY,
        100,
        0,
        2,
        null,
        null,
        false,
        100,
        5000,
        5000,
        60000,
        3
    );
    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    Assert.assertEquals(getSequenceNumber(insertDataResults, shardId1, 0), recordSupplier.getPosition(partition1));

    int i = 0;
    while (recordSupplier.bufferSize() < 3 && i++ < pollRetry) {
      Thread.sleep(100);
    }

    Assert.assertEquals(3, recordSupplier.poll(poll_timeout_millis).size());

    Assert.assertEquals(getSequenceNumber(insertDataResults, shardId1, 3), recordSupplier.getPosition(partition1));
  }


  private static String getSequenceNumber(List<PutRecordsResultEntry> entries, String shardId, int sequence)
  {
    List<PutRecordsResultEntry> sortedEntries = entries.stream()
                                                       .filter(e -> e.getShardId().equals(shardId))
                                                       .sorted(Comparator.comparing(e -> KinesisSequenceNumber.of(e.getSequenceNumber())))
                                                       .collect(Collectors.toList());
    return sortedEntries.get(sequence).getSequenceNumber();
  }
}
