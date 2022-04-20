package org.apache.druid.indexing.pulsar;

import com.google.common.collect.ImmutableSet;
import io.vavr.Function2;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PulsarRecordSupplierTest  extends EasyMockSupport {

  private static String topic = "topic";

  private static PulsarClient pulsarClient;

  @BeforeClass
  public static void setupClass() throws Exception
  {
//    pulsarTestBase = new PulsarTestBase();
//    pulsarTestBase.setupCluster();
  }

  @Before
  public void setupTest() throws Exception
  {
    pulsarClient = createMock(PulsarClient.class);
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
//    pulsarTestBase.tearDown();
  }

//  private static List<ProducerRecord<byte[], byte[]>> generateRecords(String topic)
//  {
//    return ImmutableList.of(
//      new ProducerRecord<>(topic, 0, null, jb("2008", "a", "y", "10", "20.0", "1.0")),
//      new ProducerRecord<>(topic, 0, null, jb("2009", "b", "y", "10", "20.0", "1.0")),
//      new ProducerRecord<>(topic, 0, null, jb("2010", "c", "y", "10", "20.0", "1.0")),
//      new ProducerRecord<>(topic, 0, null, jb("2011", "d", "y", "10", "20.0", "1.0")),
//      new ProducerRecord<>(topic, 0, null, jb("2011", "e", "y", "10", "20.0", "1.0")),
//      new ProducerRecord<>(topic, 0, null, jb("246140482-04-24T15:36:27.903Z", "x", "z", "10", "20.0", "1.0")),
//      new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8("unparseable")),
//      new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8("unparseable2")),
//      new ProducerRecord<>(topic, 0, null, null),
//      new ProducerRecord<>(topic, 0, null, jb("2013", "f", "y", "10", "20.0", "1.0")),
//      new ProducerRecord<>(topic, 0, null, jb("2049", "f", "y", "notanumber", "20.0", "1.0")),
//      new ProducerRecord<>(topic, 1, null, jb("2049", "f", "y", "10", "notanumber", "1.0")),
//      new ProducerRecord<>(topic, 1, null, jb("2049", "f", "y", "10", "20.0", "notanumber")),
//      new ProducerRecord<>(topic, 1, null, jb("2012", "g", "y", "10", "20.0", "1.0")),
//      new ProducerRecord<>(topic, 1, null, jb("2011", "h", "y", "10", "20.0", "1.0"))
//    );
//  }

//  private static byte[] jb(String timestamp, String dim1, String dim2, String dimLong, String dimFloat, String met1)
//  {
//    try {
//      return new ObjectMapper().writeValueAsBytes(
//        ImmutableMap.builder()
//          .put("timestamp", timestamp)
//          .put("dim1", dim1)
//          .put("dim2", dim2)
//          .put("dimLong", dimLong)
//          .put("dimFloat", dimFloat)
//          .put("met1", met1)
//          .build()
//      );
//    }
//    catch (Exception e) {
//      throw new RuntimeException(e);
//    }
//  }

  @Test
  public void testSupplierSetup()
  {
    Set<StreamPartition<Integer>> partitions = ImmutableSet.of(
      StreamPartition.of(topic, 0),
      StreamPartition.of(topic, 1)
    );

    Reader<byte[]> reader = createMock(Reader.class);
    EasyMock.expect(reader.closeAsync()).andReturn(CompletableFuture.completedFuture(null)).times(2);
    EasyMock.replay(reader);

    CompletableFuture<Reader<byte[]>> completableFuture = createMock(CompletableFuture.class);

    Function2<PulsarClient, String, CompletableFuture<Reader<byte[]>>> test = (pulsarClient, topic) -> {
      return CompletableFuture.completedFuture(reader);
    };

    PulsarRecordSupplier recordSupplier = new PulsarRecordSupplier("test","test",1, pulsarClient, test);

    Assert.assertTrue(recordSupplier.getAssignment().isEmpty());

    recordSupplier.assign(partitions);

    Assert.assertEquals(partitions, recordSupplier.getAssignment());

    recordSupplier.close();
  }


}
