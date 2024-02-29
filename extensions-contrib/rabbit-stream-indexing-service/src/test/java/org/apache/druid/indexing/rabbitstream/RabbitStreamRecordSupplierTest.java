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

package org.apache.druid.indexing.rabbitstream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.codec.WrapperMessageBuilder;
import com.rabbitmq.stream.impl.Client;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.metadata.DynamicConfigProvider;
import org.apache.druid.metadata.MapStringDynamicConfigProvider;
import org.apache.druid.segment.TestHelper;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RabbitStreamRecordSupplierTest extends EasyMockSupport
{

  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();
  private static String uri = "rabbitmq-stream://localhost:5552";
  private static Environment environment;
  private static EnvironmentBuilder environmentBuilder;
  private static ClientParameters clientParameters;
  private static Client client;
  
  private static final String STREAM = "stream";
  private static final String PARTITION_ID1 = "stream-1";
  private static final String PARTITION_ID0 = "stream-0";
  private static final List<String> ALL_PARTITIONS = ImmutableList.of(PARTITION_ID0, PARTITION_ID1);

  private static class MockedRabbitStreamRecordSupplier extends RabbitStreamRecordSupplier
  {

    public ClientParameters sentParameters = null;

    public MockedRabbitStreamRecordSupplier(String uri, Map<String, Object> consumerProperties)
    {
      super(
          consumerProperties,
          OBJECT_MAPPER,
          uri,
          100,
          1000,
          100);

    }

    @Override
    public EnvironmentBuilder getEnvBuilder()
    {
      return environmentBuilder;
    }

    @Override
    public ClientParameters getParameters()
    {
      return clientParameters;
    }

    @Override
    public Client getClient(ClientParameters paramaters)
    {
      this.sentParameters = paramaters;
      return client;
    }

  }

  public MockedRabbitStreamRecordSupplier makeRecordSupplierWithMockedEnvironment(String uri, Map<String, Object> consumerProperties)
  {
    EasyMock.expect(environmentBuilder.uri(uri)).andReturn(environmentBuilder);
    EasyMock.expect(environmentBuilder.build()).andReturn(environment);
    replayAll();
    MockedRabbitStreamRecordSupplier res = new MockedRabbitStreamRecordSupplier(uri, consumerProperties);
    resetAll();
    return res;
  }

  private class MessageHandlerContext implements MessageHandler.Context
  {

    private final long offset;
    private final long timestamp;
    private final long committedOffset;
    private final String stream;

    private MessageHandlerContext(
        long offset,
        long timestamp,
        long committedOffset,
        String stream)
    {
      this.offset = offset;
      this.timestamp = timestamp;
      this.committedOffset = committedOffset;
      this.stream = stream;
    }

    @Override
    public long offset()
    {
      return this.offset;
    }

    @Override
    public void storeOffset()
    {
    }

    @Override
    public long timestamp()
    {
      return this.timestamp;
    }

    @Override
    public long committedChunkId()
    {
      return this.committedOffset;
    }

    @Override
    public String stream()
    {
      return this.stream;
    }

    @Override
    public Consumer consumer()
    {
      return createMock(Consumer.class);
    }

    @Override
    public void processed()
    {

    }
  }

  @Before
  public void setupTest()
  {
    environment = createMock(Environment.class);
    environmentBuilder = createMock(EnvironmentBuilder.class);
    client = createMock(Client.class);
    clientParameters = createMock(ClientParameters.class);
    
  }

  @Test
  public void testGetStreamFromSubstream()
  {
    String test = "stream-0";
    String res = RabbitStreamRecordSupplier.getStreamFromSubstream(test);
    Assert.assertEquals("stream", res);

    test = "test-stream-0";
    res = RabbitStreamRecordSupplier.getStreamFromSubstream(test);
    Assert.assertEquals("test-stream", res);

  }

  @Test
  public void testAssign()
  {
    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(STREAM, PARTITION_ID0),
        StreamPartition.of(STREAM, PARTITION_ID1));

    Set<StreamPartition<String>> res;

    RabbitStreamRecordSupplier recordSupplier = makeRecordSupplierWithMockedEnvironment(
        uri,
        null
    );


    res = recordSupplier.getAssignment();

    Assert.assertTrue(res.isEmpty());

    EasyMock.expect(environmentBuilder.uri("rabbitmq-stream://localhost:5552")).andReturn(environmentBuilder).once();
    EasyMock.expect(environmentBuilder.build()).andStubReturn(environment);

    ConsumerBuilder consumerBuilderMock1 = createMock(ConsumerBuilder.class);
    EasyMock.expect(environment.consumerBuilder()).andReturn(consumerBuilderMock1).once();
    EasyMock.expect(consumerBuilderMock1.noTrackingStrategy()).andReturn(consumerBuilderMock1).once();
    EasyMock.expect(consumerBuilderMock1.stream(PARTITION_ID0)).andReturn(consumerBuilderMock1).once();
    EasyMock.expect(consumerBuilderMock1.messageHandler(recordSupplier)).andReturn(consumerBuilderMock1).once();

    ConsumerBuilder consumerBuilderMock2 = createMock(ConsumerBuilder.class);
    EasyMock.expect(environment.consumerBuilder()).andReturn(consumerBuilderMock2).once();
    EasyMock.expect(consumerBuilderMock2.noTrackingStrategy()).andReturn(consumerBuilderMock2).once();
    EasyMock.expect(consumerBuilderMock2.stream(PARTITION_ID1)).andReturn(consumerBuilderMock2).once();
    EasyMock.expect(consumerBuilderMock2.messageHandler(recordSupplier)).andReturn(consumerBuilderMock2).once();

    replayAll();

    recordSupplier.assign(partitions);
    Assert.assertEquals(partitions, recordSupplier.getAssignment());

    verifyAll();
  }

  @Test
  public void testAssignClears()
  {
    StreamPartition<String> partition0 = StreamPartition.of(STREAM, PARTITION_ID0);
    StreamPartition<String> partition1 = StreamPartition.of(STREAM, PARTITION_ID1);
    long offset1 = 1;
    long offset2 = 2;

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(STREAM, PARTITION_ID0),
        StreamPartition.of(STREAM, PARTITION_ID1));

    Set<StreamPartition<String>> res;

    RabbitStreamRecordSupplier recordSupplier = makeRecordSupplierWithMockedEnvironment(
        uri,
        null
    );


    res = recordSupplier.getAssignment();

    Assert.assertTrue(res.isEmpty());

    EasyMock.expect(environmentBuilder.uri("rabbitmq-stream://localhost:5552")).andReturn(environmentBuilder).once();
    EasyMock.expect(environmentBuilder.build()).andStubReturn(environment);

    ConsumerBuilder consumerBuilderMock1 = createMock(ConsumerBuilder.class);
    EasyMock.expect(environment.consumerBuilder()).andReturn(consumerBuilderMock1).once();

    EasyMock.expect(consumerBuilderMock1.noTrackingStrategy()).andReturn(consumerBuilderMock1).once();
    EasyMock.expect(consumerBuilderMock1.stream(PARTITION_ID0)).andReturn(consumerBuilderMock1).once();
    EasyMock.expect(consumerBuilderMock1.messageHandler(recordSupplier)).andReturn(consumerBuilderMock1).once();

    ConsumerBuilder consumerBuilderMock2 = createMock(ConsumerBuilder.class);
    EasyMock.expect(environment.consumerBuilder()).andReturn(consumerBuilderMock2).once();
    EasyMock.expect(consumerBuilderMock2.noTrackingStrategy()).andReturn(consumerBuilderMock2).once();
    EasyMock.expect(consumerBuilderMock2.stream(PARTITION_ID1)).andReturn(consumerBuilderMock2).once();
    EasyMock.expect(consumerBuilderMock2.messageHandler(recordSupplier)).andReturn(consumerBuilderMock2).once();

    replayAll();

    recordSupplier.assign(partitions);
    Assert.assertEquals(partitions, recordSupplier.getAssignment());

    verifyAll();

    try {
      recordSupplier.seek(partition0, offset1);
      recordSupplier.seek(partition1, offset2);
    }
    catch (Exception exc) {
      Assert.fail("Exception seeking:" + exc.getMessage());
    }

    resetAll();

    EasyMock.expect(environment.consumerBuilder()).andReturn(consumerBuilderMock1).once();
    EasyMock.expect(consumerBuilderMock1.noTrackingStrategy()).andReturn(consumerBuilderMock1).once();
    EasyMock.expect(consumerBuilderMock1.stream(PARTITION_ID0)).andReturn(consumerBuilderMock1).once();
    EasyMock.expect(consumerBuilderMock1.messageHandler(recordSupplier)).andReturn(consumerBuilderMock1).once();
    replayAll();

    recordSupplier.assign(ImmutableSet.of(partition0));
    Assert.assertEquals(ImmutableSet.of(partition0), recordSupplier.getAssignment());
  
    Assert.assertNotNull(recordSupplier.getOffset(partition0));
    Assert.assertNull(recordSupplier.getOffset(partition1));

  }

  @Test
  public void testSeek()
  {
    StreamPartition<String> partition0 = StreamPartition.of(STREAM, PARTITION_ID0);
    StreamPartition<String> partition1 = StreamPartition.of(STREAM, PARTITION_ID1);

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        partition0,
        partition1);

    long offset1 = 1;
    long offset2 = 2;


    RabbitStreamRecordSupplier recordSupplier = makeRecordSupplierWithMockedEnvironment(
        uri,
        null
    );

    EasyMock.expect(environmentBuilder.uri("rabbitmq-stream://localhost:5552")).andReturn(environmentBuilder).once();
    EasyMock.expect(environmentBuilder.build()).andStubReturn(environment);

    ConsumerBuilder consumerBuilderMock1 = createMock(ConsumerBuilder.class);
    EasyMock.expect(environment.consumerBuilder()).andReturn(consumerBuilderMock1).once();

    EasyMock.expect(consumerBuilderMock1.noTrackingStrategy()).andReturn(consumerBuilderMock1).once();
    EasyMock.expect(consumerBuilderMock1.stream(PARTITION_ID0)).andReturn(consumerBuilderMock1).once();
    EasyMock.expect(consumerBuilderMock1.messageHandler(recordSupplier)).andReturn(consumerBuilderMock1).once();

    ConsumerBuilder consumerBuilderMock2 = createMock(ConsumerBuilder.class);
    EasyMock.expect(environment.consumerBuilder()).andReturn(consumerBuilderMock2).once();
    EasyMock.expect(consumerBuilderMock2.noTrackingStrategy()).andReturn(consumerBuilderMock2).once();
    EasyMock.expect(consumerBuilderMock2.stream(PARTITION_ID1)).andReturn(consumerBuilderMock2).once();
    EasyMock.expect(consumerBuilderMock2.messageHandler(recordSupplier)).andReturn(consumerBuilderMock2).once();

    replayAll();

    recordSupplier.assign(partitions);
    Assert.assertEquals(partitions, recordSupplier.getAssignment());

    verifyAll();

    try {
      recordSupplier.seek(partition0, offset1);
      recordSupplier.seek(partition1, offset2);
    }
    catch (Exception exc) {
      Assert.fail("Exception seeking:" + exc.getMessage());
    }


    Assert.assertEquals(recordSupplier.getOffset(partition0).getOffset(), offset1);
    Assert.assertEquals(recordSupplier.getOffset(partition1).getOffset(), offset2);

    try {
      recordSupplier.seekToEarliest(partitions);
    }
    catch (Exception exc) {
      Assert.fail("Exception seeking:" + exc.getMessage());
    }
    Assert.assertEquals(recordSupplier.getOffset(partition0), OffsetSpecification.first());
    Assert.assertEquals(recordSupplier.getOffset(partition1), OffsetSpecification.first());

    try {
      recordSupplier.seekToLatest(partitions);
    }
    catch (Exception exc) {
      Assert.fail("Exception seeking:" + exc.getMessage());
    }

    Assert.assertEquals(recordSupplier.getOffset(partition0), OffsetSpecification.last());
    Assert.assertEquals(recordSupplier.getOffset(partition1), OffsetSpecification.last());

  }



  @Test
  public void testPollBothPartitions()
  {

    StreamPartition<String> partition1 = StreamPartition.of(STREAM, PARTITION_ID0);
    StreamPartition<String> partition2 = StreamPartition.of(STREAM, PARTITION_ID1);

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        partition1,
        partition2);

    long offset1 = 1;
    long offset2 = 2;

    RabbitStreamRecordSupplier recordSupplier = makeRecordSupplierWithMockedEnvironment(
        uri,
        null
    );

    EasyMock.expect(environmentBuilder.uri("rabbitmq-stream://localhost:5552")).andReturn(environmentBuilder).once();
    EasyMock.expect(environmentBuilder.build()).andStubReturn(environment);

    ConsumerBuilder consumerBuilderMock1 = createMock(ConsumerBuilder.class);
    EasyMock.expect(environment.consumerBuilder()).andReturn(consumerBuilderMock1).once();
    EasyMock.expect(consumerBuilderMock1.noTrackingStrategy()).andReturn(consumerBuilderMock1).once();
    EasyMock.expect(consumerBuilderMock1.stream(PARTITION_ID0)).andReturn(consumerBuilderMock1).once();
    EasyMock.expect(consumerBuilderMock1.messageHandler(recordSupplier)).andReturn(consumerBuilderMock1).once();

    EasyMock.expect(consumerBuilderMock1.offset(OffsetSpecification.offset(offset1))).andReturn(consumerBuilderMock1)
        .once();

    Consumer consumer1 = createMock(Consumer.class);
    consumer1.close();

    MessageHandler.Context mockMessageContext1 = new MessageHandlerContext(0, 0, 0, PARTITION_ID0);
    WrapperMessageBuilder mockMessageBuilder = new WrapperMessageBuilder();
    mockMessageBuilder.addData("Hello shard0".getBytes(StandardCharsets.UTF_8));

    // when the consumerbuilder is turned into a consumer, call the handler method
    // to test round trip and ensure messages come back
    EasyMock.expect(consumerBuilderMock1.build()).andAnswer(() -> {
      recordSupplier.handle(
          mockMessageContext1,
          mockMessageBuilder.build());
      return consumer1;
    }).once();

    ConsumerBuilder consumerBuilderMock2 = createMock(ConsumerBuilder.class);
    EasyMock.expect(environment.consumerBuilder()).andReturn(consumerBuilderMock2).once();
    EasyMock.expect(consumerBuilderMock2.noTrackingStrategy()).andReturn(consumerBuilderMock2).once();
    EasyMock.expect(consumerBuilderMock2.stream(PARTITION_ID1)).andReturn(consumerBuilderMock2).once();
    EasyMock.expect(consumerBuilderMock2.messageHandler(recordSupplier)).andReturn(consumerBuilderMock2).once();

    EasyMock.expect(consumerBuilderMock2.offset(OffsetSpecification.offset(offset2))).andReturn(consumerBuilderMock2)
        .once();

    Consumer consumer2 = createMock(Consumer.class);
    consumer2.close();

    MessageHandler.Context mockMessageContext2 = new MessageHandlerContext(0, 0, 0, PARTITION_ID1);
    WrapperMessageBuilder mockMessageBuilder2 = new WrapperMessageBuilder();
    mockMessageBuilder2.addData("Hello shard1".getBytes(StandardCharsets.UTF_8));

    EasyMock.expect(consumerBuilderMock2.build()).andAnswer(() -> {
      recordSupplier.handle(
          mockMessageContext2,
          mockMessageBuilder2.build());
      return consumer2;
    }).once();

    replayAll();

    recordSupplier.assign(partitions);

    try {
      recordSupplier.seek(partition1, offset1);
      recordSupplier.seek(partition2, offset2);
    }
    catch (Exception exc) {
      Assert.fail("Exception seeking:" + exc.getMessage());
    }

    List<OrderedPartitionableRecord<String, Long, ByteEntity>> messages = recordSupplier.poll(0);
    Assert.assertEquals(2, messages.size());

    recordSupplier.close();

    // test double close
    recordSupplier.close();

    verifyAll();
  }


  @Test
  public void testConsumerProperties()
  {

    DynamicConfigProvider dynamicConfigProvider = new MapStringDynamicConfigProvider(
        ImmutableMap.of(
            "username", "RABBIT_USERNAME",
            "password", "RABBIT_PASSWORD"
        )
    );
    Map<String, Object> consumerProperties = ImmutableMap.of(
        "druid.dynamic.config.provider", OBJECT_MAPPER.convertValue(dynamicConfigProvider, Map.class)
    );
    

    EasyMock.expect(environmentBuilder.uri("rabbitmq-stream://localhost:5552")).andReturn(environmentBuilder).once();
    EasyMock.expect(environmentBuilder.password("RABBIT_PASSWORD")).andReturn(environmentBuilder).once();
    EasyMock.expect(environmentBuilder.username("RABBIT_USERNAME")).andReturn(environmentBuilder).once();
    EasyMock.expect(environmentBuilder.build()).andReturn(environment).once();
    replayAll();

    MockedRabbitStreamRecordSupplier supplier = new MockedRabbitStreamRecordSupplier("rabbitmq-stream://localhost:5552", consumerProperties);
    supplier.getRabbitEnvironment();
    verifyAll();
    supplier.close();
   

    
  }


  @Test
  public void testGetPartitionIDs()
  {
    EasyMock.expect(environmentBuilder.uri("rabbitmq-stream://localhost:5552")).andReturn(environmentBuilder).once();
    EasyMock.expect(environmentBuilder.build()).andReturn(environment).once();
    replayAll();

    MockedRabbitStreamRecordSupplier supplier = new MockedRabbitStreamRecordSupplier("rabbitmq-stream://localhost:5552", null);
    supplier.getRabbitEnvironment();
    verifyAll();
    resetAll();

    EasyMock.expect(clientParameters.host("localhost")).andReturn(clientParameters);
    EasyMock.expect(clientParameters.port(5552)).andReturn(clientParameters);

    
    EasyMock.expect(client.partitions(STREAM)).andReturn(ALL_PARTITIONS);
    
    client.close();
    replayAll();

    Set<String> partitions = supplier.getPartitionIds(STREAM);
    verifyAll();

    Assert.assertTrue(clientParameters == supplier.sentParameters);

    Assert.assertEquals(2, partitions.size());
    Assert.assertTrue(partitions.containsAll(ALL_PARTITIONS));
   
    supplier.close();
    
  }

  @Test
  public void testGetPartitionIDsWithConfig()
  {

    DynamicConfigProvider dynamicConfigProvider = new MapStringDynamicConfigProvider(
        ImmutableMap.of(
        "username", "RABBIT_USERNAME",
        "password", "RABBIT_PASSWORD"
        )
    );
    Map<String, Object> consumerProperties = ImmutableMap.of(
          "druid.dynamic.config.provider", OBJECT_MAPPER.convertValue(dynamicConfigProvider, Map.class)
    );
    

    EasyMock.expect(environmentBuilder.uri("rabbitmq-stream://localhost:5552")).andReturn(environmentBuilder).once();
    EasyMock.expect(environmentBuilder.password("RABBIT_PASSWORD")).andReturn(environmentBuilder).once();
    EasyMock.expect(environmentBuilder.username("RABBIT_USERNAME")).andReturn(environmentBuilder).once();
    EasyMock.expect(environmentBuilder.build()).andReturn(environment).once();
    replayAll();

    MockedRabbitStreamRecordSupplier supplier = new MockedRabbitStreamRecordSupplier("rabbitmq-stream://localhost:5552", consumerProperties);
    supplier.getRabbitEnvironment();
    verifyAll();
    resetAll();

    
    EasyMock.expect(clientParameters.host("localhost")).andReturn(clientParameters);
    EasyMock.expect(clientParameters.port(5552)).andReturn(clientParameters);
    EasyMock.expect(clientParameters.password("RABBIT_PASSWORD")).andReturn(clientParameters);
    EasyMock.expect(clientParameters.username("RABBIT_USERNAME")).andReturn(clientParameters);
    
    EasyMock.expect(client.partitions(STREAM)).andReturn(ALL_PARTITIONS);
    
    client.close();
    replayAll();

    Set<String> partitions = supplier.getPartitionIds(STREAM);
    verifyAll();

    Assert.assertTrue(clientParameters == supplier.sentParameters);

    Assert.assertEquals(2, partitions.size());
    Assert.assertTrue(partitions.containsAll(ALL_PARTITIONS));
   
    supplier.close();
    
  }

}
