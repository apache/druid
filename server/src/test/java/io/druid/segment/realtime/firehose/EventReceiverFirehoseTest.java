/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.realtime.firehose;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import io.druid.concurrent.Execs;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.ISE;
import io.druid.server.metrics.EventReceiverFirehoseMetric;
import io.druid.server.metrics.EventReceiverFirehoseRegister;
import org.apache.commons.io.IOUtils;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class EventReceiverFirehoseTest
{
  private static final int CAPACITY = 300;
  private static final int NUM_EVENTS = 100;
  private static final String SERVICE_NAME = "test_firehose";

  private final String inputRow = "[{\n"
                                  + "  \"timestamp\":123,\n"
                                  + "  \"d1\":\"v1\"\n"
                                  + "}]";

  private EventReceiverFirehoseFactory eventReceiverFirehoseFactory;
  private EventReceiverFirehoseFactory.EventReceiverFirehose firehose;
  private EventReceiverFirehoseRegister register = new EventReceiverFirehoseRegister();
  private HttpServletRequest req;

  @Before
  public void setUp() throws Exception
  {
    req = EasyMock.createMock(HttpServletRequest.class);
    eventReceiverFirehoseFactory = new EventReceiverFirehoseFactory(
        SERVICE_NAME,
        CAPACITY,
        null,
        new DefaultObjectMapper(),
        new DefaultObjectMapper(),
        register
    );
    firehose = (EventReceiverFirehoseFactory.EventReceiverFirehose) eventReceiverFirehoseFactory.connect(
        new MapInputRowParser(
            new JSONParseSpec(
                new TimestampSpec(
                    "timestamp",
                    "auto",
                    null
                ), new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("d1")), null, null),
                null,
                null
            )
        )
    );
  }

  @Test
  public void testSingleThread() throws IOException
  {
    EasyMock.expect(req.getContentType()).andReturn("application/json").times(NUM_EVENTS);
    EasyMock.replay(req);

    for (int i = 0; i < NUM_EVENTS; ++i) {
      final InputStream inputStream = IOUtils.toInputStream(inputRow);
      firehose.addAll(inputStream, req);
      Assert.assertEquals(i + 1, firehose.getCurrentBufferSize());
      inputStream.close();
    }

    EasyMock.verify(req);

    final Iterable<Map.Entry<String, EventReceiverFirehoseMetric>> metrics = register.getMetrics();
    Assert.assertEquals(1, Iterables.size(metrics));

    final Map.Entry<String, EventReceiverFirehoseMetric> entry = Iterables.getLast(metrics);
    Assert.assertEquals(SERVICE_NAME, entry.getKey());
    Assert.assertEquals(CAPACITY, entry.getValue().getCapacity());
    Assert.assertEquals(CAPACITY, firehose.getCapacity());
    Assert.assertEquals(NUM_EVENTS, entry.getValue().getCurrentBufferSize());
    Assert.assertEquals(NUM_EVENTS, firehose.getCurrentBufferSize());

    for (int i = NUM_EVENTS - 1; i >= 0; --i) {
      Assert.assertTrue(firehose.hasMore());
      Assert.assertNotNull(firehose.nextRow());
      Assert.assertEquals(i, firehose.getCurrentBufferSize());
    }

    Assert.assertEquals(CAPACITY, entry.getValue().getCapacity());
    Assert.assertEquals(CAPACITY, firehose.getCapacity());
    Assert.assertEquals(0, entry.getValue().getCurrentBufferSize());
    Assert.assertEquals(0, firehose.getCurrentBufferSize());

    firehose.close();
    Assert.assertFalse(firehose.hasMore());
    Assert.assertEquals(0, Iterables.size(register.getMetrics()));

  }

  @Test
  public void testMultipleThreads() throws InterruptedException, IOException, TimeoutException, ExecutionException
  {
    EasyMock.expect(req.getContentType()).andReturn("application/json").times(2 * NUM_EVENTS);
    EasyMock.replay(req);

    final ExecutorService executorService = Execs.singleThreaded("single_thread");
    final Future future = executorService.submit(
        new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            for (int i = 0; i < NUM_EVENTS; ++i) {
              final InputStream inputStream = IOUtils.toInputStream(inputRow);
              firehose.addAll(inputStream, req);
              inputStream.close();
            }
            return true;
          }
        }
    );

    for (int i = 0; i < NUM_EVENTS; ++i) {
      final InputStream inputStream = IOUtils.toInputStream(inputRow);
      firehose.addAll(inputStream, req);
      inputStream.close();
    }

    future.get(10, TimeUnit.SECONDS);

    EasyMock.verify(req);

    final Iterable<Map.Entry<String, EventReceiverFirehoseMetric>> metrics = register.getMetrics();
    Assert.assertEquals(1, Iterables.size(metrics));

    final Map.Entry<String, EventReceiverFirehoseMetric> entry = Iterables.getLast(metrics);
    
    Assert.assertEquals(SERVICE_NAME, entry.getKey());
    Assert.assertEquals(CAPACITY, entry.getValue().getCapacity());
    Assert.assertEquals(CAPACITY, firehose.getCapacity());
    Assert.assertEquals(2 * NUM_EVENTS, entry.getValue().getCurrentBufferSize());
    Assert.assertEquals(2 * NUM_EVENTS, firehose.getCurrentBufferSize());

    for (int i = 2 * NUM_EVENTS - 1; i >= 0; --i) {
      Assert.assertTrue(firehose.hasMore());
      Assert.assertNotNull(firehose.nextRow());
      Assert.assertEquals(i, firehose.getCurrentBufferSize());
    }

    Assert.assertEquals(CAPACITY, entry.getValue().getCapacity());
    Assert.assertEquals(CAPACITY, firehose.getCapacity());
    Assert.assertEquals(0, entry.getValue().getCurrentBufferSize());
    Assert.assertEquals(0, firehose.getCurrentBufferSize());

    firehose.close();
    Assert.assertFalse(firehose.hasMore());
    Assert.assertEquals(0, Iterables.size(register.getMetrics()));

    executorService.shutdownNow();
  }

  @Test(expected = ISE.class)
  public void testDuplicateRegistering() throws IOException
  {
    EventReceiverFirehoseFactory eventReceiverFirehoseFactory2 = new EventReceiverFirehoseFactory(
        SERVICE_NAME,
        CAPACITY,
        null,
        new DefaultObjectMapper(),
        new DefaultObjectMapper(),
        register
    );
    EventReceiverFirehoseFactory.EventReceiverFirehose firehose2 =
        (EventReceiverFirehoseFactory.EventReceiverFirehose) eventReceiverFirehoseFactory2
            .connect(
                new MapInputRowParser(
                    new JSONParseSpec(
                        new TimestampSpec(
                            "timestamp",
                            "auto",
                            null
                        ), new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("d1")), null, null),
                        null,
                        null
                    )
                )
            );
  }

  @Test(timeout = 40_000L)
  public void testShutdownWithPrevTime() throws Exception
  {
    firehose.shutdown(DateTime.now().minusMinutes(2).toString());
    while (!firehose.isClosed()){
      Thread.sleep(50);
    }
  }

  @Test(timeout = 40_000L)
  public void testShutdown() throws Exception
  {
    firehose.shutdown(DateTime.now().plusMillis(100).toString());
    while (!firehose.isClosed()){
     Thread.sleep(50);
    }
  }
}
