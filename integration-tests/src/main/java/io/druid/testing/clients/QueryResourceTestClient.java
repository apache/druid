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
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.guice.annotations.Global;
import io.druid.query.Query;
import io.druid.testing.IntegrationTestingConfig;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class QueryResourceTestClient
{
  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final String router;
  private final StatusResponseHandler responseHandler;

  @Inject
  QueryResourceTestClient(
      ObjectMapper jsonMapper,
      @Global HttpClient httpClient,
      IntegrationTestingConfig config
  )
  {
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.router = config.getRouterHost();
    this.responseHandler = new StatusResponseHandler(Charsets.UTF_8);
  }

  private String getBrokerURL()
  {
    return String.format(
        "http://%s/druid/v2/",
        router
    );
  }

  public List<Map<String, Object>> query(Query query)
  {
    try {
      StatusResponseHolder response = httpClient.post(new URL(getBrokerURL()))
                                                .setContent(
                                                    "application/json",
                                                    jsonMapper.writeValueAsBytes(query)
                                                )
                                                .go(responseHandler)
                                                .get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while querying[%s] status[%s] content[%s]",
            getBrokerURL(),
            response.getStatus(),
            response.getContent()
        );
      }

      return jsonMapper.readValue(
          response.getContent(), new TypeReference<List<Map<String, Object>>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
