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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.client.TestSegmentChangeRequestHttpClient;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.segment.TestHelper;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.junit.Test;

import java.net.URL;
import java.util.Arrays;

/**
 *
 */
public class ChangeRequestHttpSyncerTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();
  private static final TypeReference<ChangeRequestsSnapshot<String>> TYPE_REF
      = new TypeReference<ChangeRequestsSnapshot<String>>()
  {
  };

  @Test(timeout = 60_000L)
  public void testSimple() throws Exception
  {
    TestSegmentChangeRequestHttpClient httpClient = new TestSegmentChangeRequestHttpClient();
    httpClient.addNextError(new ISE("Could not send request to server"));
    httpClient.addNextError(new ISE("Unexpected response from server"));
    httpClient.addNextResult(buildRequestSnapshot("s1"), TYPE_REF);
    httpClient.addNextResult(buildRequestSnapshot("s2"), TYPE_REF);
    httpClient.addNextResult(
        new ChangeRequestsSnapshot<>(
            true,
            "reset the counter",
            ChangeRequestHistory.Counter.ZERO,
            ImmutableList.of()
        ),
        TYPE_REF
    );
    httpClient.addNextResult(buildRequestSnapshot("s3"), TYPE_REF);
    httpClient.addNextResult(buildRequestSnapshot("s4"), TYPE_REF);

    ChangeRequestHttpSyncer.Listener<String> listener = EasyMock.mock(ChangeRequestHttpSyncer.Listener.class);
    listener.fullSync(ImmutableList.of("s1"));
    listener.deltaSync(ImmutableList.of("s2"));
    listener.fullSync(ImmutableList.of("s3"));
    listener.deltaSync(ImmutableList.of("s4"));
    EasyMock.replay(listener);

    ChangeRequestHttpSyncer<String> syncer = new ChangeRequestHttpSyncer<>(
        MAPPER,
        httpClient,
        Execs.scheduledSingleThreaded("ChangeRequestHttpSyncerTest"),
        new URL("http://localhost:8080/"),
        "/xx",
        TYPE_REF,
        Duration.standardSeconds(50),
        Duration.standardSeconds(10),
        listener
    );

    syncer.start();

    while (httpClient.hasPendingResults()) {
      Thread.sleep(100);
    }

    syncer.stop();

    EasyMock.verify(listener);
  }

  private ChangeRequestsSnapshot<String> buildRequestSnapshot(String... requests)
  {
    return new ChangeRequestsSnapshot<>(
        false,
        null,
        ChangeRequestHistory.Counter.ZERO,
        Arrays.asList(requests)
    );
  }
}
