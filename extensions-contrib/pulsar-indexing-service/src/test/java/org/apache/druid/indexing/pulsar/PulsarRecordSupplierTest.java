package org.apache.druid.indexing.pulsar;

import com.google.common.collect.ImmutableSet;
import io.vavr.Function3;
import org.apache.commons.io.IOUtils;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PulsarRecordSupplierTest extends EasyMockSupport
{

  private static String topic = "topic";

  private static PulsarClient pulsarClient;
  Set<StreamPartition<Integer>> partitions = ImmutableSet.of(
      StreamPartition.of(topic, 0),
      StreamPartition.of(topic, 1)
  );
  private BlockingQueue<Message<byte[]>> received;

  @BeforeClass
  public static void setupClass() throws Exception
  {
//    pulsarTestBase = new PulsarTestBase();
//    pulsarTestBase.setupCluster();
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
//    pulsarTestBase.tearDown();
  }

  @Before
  public void setupTest() throws Exception
  {
    pulsarClient = createMock(PulsarClient.class);
    received = new ArrayBlockingQueue<>(1);
  }

  @Test
  public void testSupplierSetupAndAssignment()
  {

    Reader<byte[]> reader = createMock(Reader.class);
    reader.closeAsync();
    EasyMock.expectLastCall().andReturn(CompletableFuture.completedFuture(null)).times(2);
    EasyMock.replay(reader);

    CompletableFuture<Reader<byte[]>> completableFuture = createMock(CompletableFuture.class);

    Function3<PulsarClient, String, PulsarRecordSupplier, CompletableFuture<Reader<byte[]>>> test = (pulsarClient, topic, supplier) -> {
      return CompletableFuture.completedFuture(reader);
    };

    PulsarRecordSupplier recordSupplier = new PulsarRecordSupplier("test", "test", 1, pulsarClient, test, received);

    Assert.assertTrue(recordSupplier.getAssignment().isEmpty());

    recordSupplier.assign(partitions);

    Assert.assertEquals(partitions, recordSupplier.getAssignment());

    recordSupplier.close();

    EasyMock.verify(reader);
  }

  @Test
  public void testPoll() throws InterruptedException, IOException
  {
    MessageId messageId = new MessageIdImpl(-1, -1, -1);
    String messageData = "{'k1':'v1','k2':'v2'}";
    Reader<byte[]> reader = createMock(Reader.class);

    CompletableFuture<Reader<byte[]>> completableFuture = createMock(CompletableFuture.class);

    Function3<PulsarClient, String, PulsarRecordSupplier, CompletableFuture<Reader<byte[]>>> test = (pulsarClient, topic, supplier) -> {
      return CompletableFuture.completedFuture(reader);
    };

    Message<byte[]> message = createMock(Message.class);
    EasyMock.expect(message.getTopicName()).andReturn("test");
    EasyMock.expect(message.getValue()).andReturn(messageData.getBytes(StandardCharsets.UTF_8));
    PulsarSequenceNumber offset = PulsarSequenceNumber.of(messageId);
    EasyMock.expect(message.getMessageId()).andReturn(messageId);
    EasyMock.replay(message);
    List<OrderedPartitionableRecord<Integer, Long, ByteEntity>> expected = Stream
        .of(messageData)
        .map(x ->
            new OrderedPartitionableRecord<>("test",
                1,
                offset.get(),
                Collections.singletonList(new ByteEntity(x.getBytes(StandardCharsets.UTF_8)))))
        .collect(Collectors.toList());

    received.add(message);

    PulsarRecordSupplier recordSupplier = new PulsarRecordSupplier("test", "test", 1, pulsarClient, test, received);

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    List<OrderedPartitionableRecord<Integer, Long, ByteEntity>> polledRecords = recordSupplier.poll(1000);


//    verifyAll();

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(IOUtils.toString(expected.get(0).getData().get(0).open()),
        IOUtils.toString(polledRecords.get(0).getData().get(0).open()));

  }


}
