/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.master;

import com.google.common.base.Throwables;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.merge.ClientAppendQuery;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.HttpResponseHandler;
import com.netflix.curator.x.discovery.ServiceProvider;
import org.codehaus.jackson.map.ObjectMapper;

import java.net.URL;
import java.util.List;

public class HttpMergerClient implements MergerClient
{
  private final HttpClient client;
  private final HttpResponseHandler<StringBuilder, String> responseHandler;
  private final ObjectMapper jsonMapper;
  private final ServiceProvider serviceProvider;

  public HttpMergerClient(
      HttpClient client,
      HttpResponseHandler<StringBuilder, String> responseHandler,
      ObjectMapper jsonMapper,
      ServiceProvider serviceProvider
  )
  {
    this.client = client;
    this.responseHandler = responseHandler;
    this.jsonMapper = jsonMapper;
    this.serviceProvider = serviceProvider;
  }

  public void runRequest(String dataSource, List<DataSegment> segments)
  {
    try {
      byte[] dataToSend = jsonMapper.writeValueAsBytes(
          new ClientAppendQuery(dataSource, segments)
      );

      client.post(
          new URL(
              String.format(
                  "http://%s:%s/mmx/merger/v1/merge",
                  serviceProvider.getInstance().getAddress(),
                  serviceProvider.getInstance().getPort()
              )
          )
      )
            .setContent("application/json", dataToSend)
            .go(responseHandler)
            .get();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
