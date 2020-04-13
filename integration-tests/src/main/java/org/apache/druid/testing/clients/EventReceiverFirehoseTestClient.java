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

package org.apache.druid.testing.clients;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.testing.guice.TestClient;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.core.MediaType;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class EventReceiverFirehoseTestClient
{
  private static final Logger LOG = new Logger(EventReceiverFirehoseTestClient.class);

  static final int NUM_RETRIES = 30;
  static final long DELAY_FOR_RETRIES_MS = 10000;

  private final String host;
  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final String chatID;
  private final ObjectMapper smileMapper;

  public EventReceiverFirehoseTestClient(
      String host,
      String chatID,
      ObjectMapper jsonMapper,
      @TestClient HttpClient httpClient,
      ObjectMapper smileMapper
  )
  {
    this.host = host;
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.chatID = chatID;
    this.smileMapper = smileMapper;
  }

  private String getURL()
  {
    return StringUtils.format(
        "https://%s/druid/worker/v1/chat/%s/push-events/",
        host,
        chatID
    );
  }

  /**
   * post events from the collection and return the count of events accepted
   *
   * @param events Collection of events to be posted
   *
   * @return
   */
  public int postEvents(Collection<Map<String, Object>> events, ObjectMapper objectMapper, String mediaType)
      throws InterruptedException
  {
    int retryCount = 0;
    while (true) {
      try {
        StatusResponseHolder response = httpClient.go(
            new Request(HttpMethod.POST, new URL(getURL()))
                .setContent(mediaType, objectMapper.writeValueAsBytes(events)),
            StatusResponseHandler.getInstance()
        ).get();

        if (!response.getStatus().equals(HttpResponseStatus.OK)) {
          throw new ISE(
              "Error while posting events to url[%s] status[%s] content[%s]",
              getURL(),
              response.getStatus(),
              response.getContent()
          );
        }
        Map<String, Integer> responseData = objectMapper.readValue(
            response.getContent(), new TypeReference<Map<String, Integer>>()
            {
            }
        );
        return responseData.get("eventCount");
      }
      // adding retries to flaky tests using channels
      catch (ExecutionException e) {
        if (retryCount > NUM_RETRIES) {
          throw new RuntimeException(e); //giving up now
        } else {
          LOG.info(e, "received exception, sleeping and retrying");
          retryCount++;
          Thread.sleep(DELAY_FOR_RETRIES_MS);
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Reads each events from file and post them to the indexing service.
   * Uses both smileMapper and jsonMapper to send events alternately.
   *
   * @param file location of file to post events from
   *
   * @return number of events sent to the indexing service
   */
  public int postEventsFromFile(String file)
  {
    try (
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(
                EventReceiverFirehoseTestClient.class.getResourceAsStream(file),
                StandardCharsets.UTF_8
            )
        )
    ) {

      String s;
      Collection<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
      // Test sending events using both jsonMapper and smileMapper.
      // sends events one by one using both jsonMapper and smileMapper.
      int totalEventsPosted = 0;
      int expectedEventsPosted = 0;
      while ((s = reader.readLine()) != null) {
        events.add(this.jsonMapper.readValue(s, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT));
        ObjectMapper mapper = (totalEventsPosted % 2 == 0) ? jsonMapper : smileMapper;
        String mediaType = (totalEventsPosted % 2 == 0)
                           ? MediaType.APPLICATION_JSON
                           : SmileMediaTypes.APPLICATION_JACKSON_SMILE;
        totalEventsPosted += postEvents(events, mapper, mediaType);

        expectedEventsPosted += events.size();
        events = new ArrayList<>();
      }

      if (totalEventsPosted != expectedEventsPosted) {
        throw new ISE("All events not posted, expected : %d actual : %d", events.size(), totalEventsPosted);
      }
      return totalEventsPosted;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

  }
}
