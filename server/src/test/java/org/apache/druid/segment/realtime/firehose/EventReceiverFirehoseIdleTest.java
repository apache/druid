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

package org.apache.druid.segment.realtime.firehose;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.metrics.EventReceiverFirehoseRegister;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import java.util.Locale;

public class EventReceiverFirehoseIdleTest
{
  private static final int CAPACITY = 300;
  private static final long MAX_IDLE_TIME = 5_000L;
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
        MAX_IDLE_TIME,
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

  @Test(timeout = 40_000L)
  public void testIdle() throws Exception
  {
    awaitFirehoseClosed();
    awaitDelayedExecutorThreadTerminated();
  }

  private void awaitFirehoseClosed() throws InterruptedException
  {
    while (!firehose.isClosed()) {
      Thread.sleep(50);
    }
  }

  private void awaitDelayedExecutorThreadTerminated() throws InterruptedException
  {
    firehose.getDelayedCloseExecutor().join();
  }

  @Test(timeout = 40_000L)
  public void testNotIdle() throws Exception
  {
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(AllowAllAuthenticator.ALLOW_ALL_RESULT)
            .anyTimes();
    EasyMock.expect(req.getHeader("X-Firehose-Producer-Id")).andReturn(null).anyTimes();
    EasyMock.expect(req.getContentType()).andReturn("application/json").anyTimes();
    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(req);

    final int checks = 5;
    for (int i = 0; i < checks; i++) {
      Assert.assertFalse(firehose.isClosed());
      System.out.printf(Locale.ENGLISH, "Check %d/%d passed\n", i + 1, checks);
      firehose.addAll(IOUtils.toInputStream(inputRow), req);
      Thread.sleep(3_000L);
    }

    awaitFirehoseClosed();
    awaitDelayedExecutorThreadTerminated();
  }
}
