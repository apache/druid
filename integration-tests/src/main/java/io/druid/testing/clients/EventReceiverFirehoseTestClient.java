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

package io.druid.testing.clients;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.testing.guice.TestClient;
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

public class EventReceiverFirehoseTestClient
{
  private final String host;
  private final StatusResponseHandler responseHandler;
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
    this.responseHandler = new StatusResponseHandler(Charsets.UTF_8);
    this.httpClient = httpClient;
    this.chatID = chatID;
    this.smileMapper = smileMapper;
  }

  private String getURL()
  {
    return StringUtils.format(
        "http://%s/druid/worker/v1/chat/%s/push-events/",
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
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.POST, new URL(getURL())
          ).setContent(
              mediaType,
              objectMapper.writeValueAsBytes(events)
          ),
          responseHandler
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
    catch (Exception e) {
      throw Throwables.propagate(e);
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
        events.add(
            (Map<String, Object>) this.jsonMapper.readValue(
                s, new TypeReference<Map<String, Object>>()
                {
                }
            )
        );
        ObjectMapper mapper = (totalEventsPosted % 2 == 0) ? jsonMapper : smileMapper;
        String mediaType = (totalEventsPosted % 2 == 0)
                           ? MediaType.APPLICATION_JSON
                           : SmileMediaTypes.APPLICATION_JACKSON_SMILE;
        totalEventsPosted += postEvents(events, mapper, mediaType);
        ;
        expectedEventsPosted += events.size();
        events = new ArrayList<>();
      }

      if (totalEventsPosted != expectedEventsPosted) {
        throw new ISE("All events not posted, expected : %d actual : %d", events.size(), totalEventsPosted);
      }
      return totalEventsPosted;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

  }
}
