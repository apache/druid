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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
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
  private static long poll_timeout_millis = 1000;
  private static String shardId1 = "shardId-000000000001";
  private static String shardId0 = "shardId-000000000000";
  private static int streamPosFix = 0;
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

  @Test
  public void testSupplierSetup() throws InterruptedException
  {

    getKinesisClientInstance();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(stream, shardId0),
        StreamPartition.of(stream, shardId1)
    );

    KinesisRecordSupplier recordSupplier = new KinesisRecordSupplier(
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
        60000
    );

    Assert.assertTrue(recordSupplier.getAssignment().isEmpty());

    recordSupplier.assign(partitions);

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(ImmutableSet.of(shardId1, shardId0), recordSupplier.getPartitionIds(stream));
    Assert.assertNull(recordSupplier.poll(100));

    recordSupplier.close();
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

    KinesisRecordSupplier recordSupplier = new KinesisRecordSupplier(
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
        60000
    );
    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    Set<OrderedPartitionableRecord<String, String>> supplierRecords = new HashSet<>();
    OrderedPartitionableRecord<String, String> record = recordSupplier.poll(poll_timeout_millis);

    while (record != null) {
      supplierRecords.add(record);
      record = recordSupplier.poll(poll_timeout_millis);
    }

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(initialRecords.size(), supplierRecords.size());
    Assert.assertTrue(supplierRecords.containsAll(initialRecords));

    recordSupplier.close();
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

    KinesisRecordSupplier recordSupplier = new KinesisRecordSupplier(
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
        60000
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    Set<OrderedPartitionableRecord<String, String>> supplierRecords = new HashSet<>();
    OrderedPartitionableRecord<String, String> record = recordSupplier.poll(poll_timeout_millis);

    while (record != null) {
      supplierRecords.add(record);
      record = recordSupplier.poll(poll_timeout_millis);
    }

    List<PutRecordsResultEntry> insertDataResults2 = insertData(kinesis, generateRecordsRequests(stream, 5, 12));
    insertDataResults2.forEach(entry -> initialRecords.add(new OrderedPartitionableRecord<>(
        stream,
        entry.getShardId(),
        entry.getSequenceNumber(),
        null
    )));

    record = recordSupplier.poll(poll_timeout_millis);
    while (record != null) {
      supplierRecords.add(record);
      record = recordSupplier.poll(poll_timeout_millis);
    }

    Assert.assertEquals(initialRecords.size(), supplierRecords.size());
    Assert.assertTrue(supplierRecords.containsAll(initialRecords));

    recordSupplier.close();
  }

  @Test
  public void testSeek() throws InterruptedException, TimeoutException
  {
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> insertDataResults = insertData(kinesis, generateRecordsRequests(stream));

    StreamPartition<String> shard0 = StreamPartition.of(stream, shardId0);
    StreamPartition<String> shard1 = StreamPartition.of(stream, shardId1);
    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        shard0,
        shard1
    );

    KinesisRecordSupplier recordSupplier = new KinesisRecordSupplier(
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
        60000
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

    Set<OrderedPartitionableRecord<String, String>> supplierRecords = new HashSet<>();
    OrderedPartitionableRecord<String, String> record = recordSupplier.poll(poll_timeout_millis);

    while (record != null) {
      supplierRecords.add(record);
      record = recordSupplier.poll(poll_timeout_millis);
    }

    Assert.assertEquals(8, supplierRecords.size());
    Assert.assertTrue(supplierRecords.containsAll(initialRecords1));
    Assert.assertTrue(supplierRecords.containsAll(initialRecords2));

    recordSupplier.close();

  }

  @Test
  public void testSeekToLatest() throws InterruptedException, TimeoutException
  {
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> insertDataResults = insertData(kinesis, generateRecordsRequests(stream));

    StreamPartition<String> shard0 = StreamPartition.of(stream, shardId0);
    StreamPartition<String> shard1 = StreamPartition.of(stream, shardId1);
    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        shard0,
        shard1
    );

    KinesisRecordSupplier recordSupplier = new KinesisRecordSupplier(
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
        60000
    );

    recordSupplier.assign(partitions);

    Assert.assertEquals(insertDataResults.get(0).getSequenceNumber(), recordSupplier.getEarliestSequenceNumber(shard1));
    Assert.assertEquals(insertDataResults.get(8).getSequenceNumber(), recordSupplier.getEarliestSequenceNumber(shard0));

    recordSupplier.seekToLatest(partitions);
    Assert.assertNull(recordSupplier.poll(poll_timeout_millis));

    recordSupplier.close();
  }

  @Test(expected = ISE.class)
  public void testSeekUnassigned() throws InterruptedException, TimeoutException
  {
    AmazonKinesis kinesis = getKinesisClientInstance();
    List<PutRecordsResultEntry> insertDataResults = insertData(kinesis, generateRecordsRequests(stream));

    StreamPartition<String> shard0 = StreamPartition.of(stream, shardId0);
    StreamPartition<String> shard1 = StreamPartition.of(stream, shardId1);
    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        shard1
    );

    KinesisRecordSupplier recordSupplier = new KinesisRecordSupplier(
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
        60000
    );

    recordSupplier.assign(partitions);

    Assert.assertEquals(insertDataResults.get(0).getSequenceNumber(), recordSupplier.getEarliestSequenceNumber(shard1));

    recordSupplier.seekToEarliest(Collections.singleton(shard0));

    recordSupplier.close();
  }
}
