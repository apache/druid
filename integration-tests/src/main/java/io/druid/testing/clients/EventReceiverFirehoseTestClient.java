/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.testing.clients;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Charsets;
import com.google.common.base.Throwables;
import com.metamx.common.ISE;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.core.MediaType;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
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

  public EventReceiverFirehoseTestClient(String host, String chatID, ObjectMapper jsonMapper, HttpClient httpClient)
  {
    this.host = host;
    this.jsonMapper = jsonMapper;
    this.responseHandler = new StatusResponseHandler(Charsets.UTF_8);
    this.httpClient = httpClient;
    this.chatID = chatID;
  }

  private String getURL()
  {
    return String.format(
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
  public int postEvents(Collection<Map<String, Object>> events)
  {
    try {
      StatusResponseHolder response = httpClient.post(new URL(getURL()))
                                                .setContent(
                                                    MediaType.APPLICATION_JSON,
                                                    this.jsonMapper.writeValueAsBytes(events)
                                                )
                                                .go(responseHandler)
                                                .get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while posting events to url[%s] status[%s] content[%s]",
            getURL(),
            response.getStatus(),
            response.getContent()
        );
      }
      Map<String, Integer> responseData = jsonMapper.readValue(
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

  public int postEventsFromFile(String file)
  {
    try {
      BufferedReader reader = new BufferedReader(
          new InputStreamReader(
              EventReceiverFirehoseTestClient.class.getResourceAsStream(
                  file
              )
          )
      );
      String s;
      Collection<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
      while ((s = reader.readLine()) != null) {
        events.add(
            (Map<String, Object>) this.jsonMapper.readValue(
                s, new TypeReference<Map<String, Object>>()
                {
                }
            )
        );
      }
      int eventsPosted = postEvents(events);
      if (eventsPosted != events.size()) {
        throw new ISE("All events not posted, expected : %d actual : %d", events.size(), eventsPosted);
      }
      return eventsPosted;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

  }
}
