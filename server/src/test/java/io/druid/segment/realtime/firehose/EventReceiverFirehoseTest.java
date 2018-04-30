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
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.server.metrics.EventReceiverFirehoseMetric;
import io.druid.server.metrics.EventReceiverFirehoseRegister;
import io.druid.server.security.AllowAllAuthenticator;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthTestUtils;
import org.apache.commons.io.IOUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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
  public void setUp()
  {
    req = EasyMock.createMock(HttpServletRequest.class);
    eventReceiverFirehoseFactory = new EventReceiverFirehoseFactory(
        SERVICE_NAME,
        CAPACITY,
        null,
        new DefaultObjectMapper(),
        new DefaultObjectMapper(),
        register,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER
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
        ),
        null
    );
  }

  @Test
  public void testSingleThread() throws IOException
  {
    for (int i = 0; i < NUM_EVENTS; ++i) {
      setUpRequestExpectations(null, null);
      final InputStream inputStream = IOUtils.toInputStream(inputRow, StandardCharsets.UTF_8);
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
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(AllowAllAuthenticator.ALLOW_ALL_RESULT)
            .anyTimes();
    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(req.getContentType()).andReturn("application/json").times(2 * NUM_EVENTS);
    EasyMock.expect(req.getHeader("X-Firehose-Producer-Id")).andReturn(null).times(2 * NUM_EVENTS);
    EasyMock.replay(req);

    final ExecutorService executorService = Execs.singleThreaded("single_thread");
    final Future future = executorService.submit(
        new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            for (int i = 0; i < NUM_EVENTS; ++i) {
              final InputStream inputStream = IOUtils.toInputStream(inputRow, StandardCharsets.UTF_8);
              firehose.addAll(inputStream, req);
              inputStream.close();
            }
            return true;
          }
        }
    );

    for (int i = 0; i < NUM_EVENTS; ++i) {
      final InputStream inputStream = IOUtils.toInputStream(inputRow, StandardCharsets.UTF_8);
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
  public void testDuplicateRegistering()
  {
    EventReceiverFirehoseFactory eventReceiverFirehoseFactory2 = new EventReceiverFirehoseFactory(
        SERVICE_NAME,
        CAPACITY,
        null,
        new DefaultObjectMapper(),
        new DefaultObjectMapper(),
        register,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER
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
                ),
                null
            );
  }

  @Test(timeout = 40_000L)
  public void testShutdownWithPrevTime() throws Exception
  {
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(AllowAllAuthenticator.ALLOW_ALL_RESULT)
            .anyTimes();
    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(req);

    firehose.shutdown(DateTimes.nowUtc().minusMinutes(2).toString(), req);
    while (!firehose.isClosed()) {
      Thread.sleep(50);
    }
  }

  @Test(timeout = 40_000L)
  public void testShutdown() throws Exception
  {
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(AllowAllAuthenticator.ALLOW_ALL_RESULT)
            .anyTimes();
    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(req);

    firehose.shutdown(DateTimes.nowUtc().plusMillis(100).toString(), req);
    while (!firehose.isClosed()) {
      Thread.sleep(50);
    }
  }

  @Test
  public void testProducerSequence() throws IOException
  {
    for (int i = 0; i < NUM_EVENTS; ++i) {
      setUpRequestExpectations("producer", String.valueOf(i));

      final InputStream inputStream = IOUtils.toInputStream(inputRow, StandardCharsets.UTF_8);
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
  public void testLowProducerSequence() throws IOException
  {
    for (int i = 0; i < NUM_EVENTS; ++i) {
      setUpRequestExpectations("producer", "1");

      final InputStream inputStream = IOUtils.toInputStream(inputRow, StandardCharsets.UTF_8);
      final Response response = firehose.addAll(inputStream, req);
      Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
      Assert.assertEquals(1, firehose.getCurrentBufferSize());
      inputStream.close();
    }

    EasyMock.verify(req);

    firehose.close();
  }

  @Test
  public void testMissingProducerSequence() throws IOException
  {
    setUpRequestExpectations("producer", null);

    final InputStream inputStream = IOUtils.toInputStream(inputRow, StandardCharsets.UTF_8);
    final Response response = firehose.addAll(inputStream, req);

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());

    inputStream.close();

    EasyMock.verify(req);

    firehose.close();
  }

  @Test
  public void testTooManyProducerIds() throws IOException
  {
    for (int i = 0; i < EventReceiverFirehoseFactory.MAX_FIREHOSE_PRODUCERS - 1; i++) {
      setUpRequestExpectations("producer-" + i, "0");

      final InputStream inputStream = IOUtils.toInputStream(inputRow, StandardCharsets.UTF_8);
      final Response response = firehose.addAll(inputStream, req);
      Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
      inputStream.close();
      Assert.assertTrue(firehose.hasMore());
      Assert.assertNotNull(firehose.nextRow());
    }

    setUpRequestExpectations("toomany", "0");

    final InputStream inputStream = IOUtils.toInputStream(inputRow, StandardCharsets.UTF_8);
    final Response response = firehose.addAll(inputStream, req);
    Assert.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
    inputStream.close();

    EasyMock.verify(req);

    firehose.close();
  }

  @Test
  public void testNaNProducerSequence() throws IOException
  {
    setUpRequestExpectations("producer", "foo");

    final InputStream inputStream = IOUtils.toInputStream(inputRow, StandardCharsets.UTF_8);
    final Response response = firehose.addAll(inputStream, req);

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());

    inputStream.close();

    EasyMock.verify(req);

    firehose.close();
  }

  private void setUpRequestExpectations(String producerId, String producerSequenceValue)
  {
    EasyMock.reset(req);
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(AllowAllAuthenticator.ALLOW_ALL_RESULT)
            .anyTimes();
    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(req.getContentType()).andReturn("application/json");
    EasyMock.expect(req.getHeader("X-Firehose-Producer-Id")).andReturn(producerId);

    if (producerId != null) {
      EasyMock.expect(req.getHeader("X-Firehose-Producer-Seq")).andReturn(producerSequenceValue);
    }

    EasyMock.replay(req);
  }
}
