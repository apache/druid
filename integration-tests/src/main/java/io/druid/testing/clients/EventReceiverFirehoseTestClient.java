/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
