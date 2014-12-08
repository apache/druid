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
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.RequestBuilder;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.guice.annotations.Global;
import io.druid.testing.IntegrationTestingConfig;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Interval;

import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;

public class CoordinatorResourceTestClient
{
  private final static Logger LOG = new Logger(CoordinatorResourceTestClient.class);
  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final String coordinator;
  private final StatusResponseHandler responseHandler;

  @Inject
  CoordinatorResourceTestClient(
      ObjectMapper jsonMapper,
      @Global HttpClient httpClient, IntegrationTestingConfig config
  )
  {
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.coordinator = config.getCoordinatorHost();
    this.responseHandler = new StatusResponseHandler(Charsets.UTF_8);
  }

  private String getCoordinatorURL()
  {
    return String.format(
        "http://%s/druid/coordinator/v1/",
        coordinator
    );
  }

  private Map<String, Double> getLoadStatus()
  {
    Map<String, Double> status = null;
    try {
      StatusResponseHolder response = makeRequest(HttpMethod.GET, getCoordinatorURL() + "loadstatus");

      status = jsonMapper.readValue(
          response.getContent(), new TypeReference<Map<String, Double>>()
          {
          }
      );
    }
    catch (Exception e) {
      Throwables.propagate(e);
    }
    return status;
  }

  public boolean areSegmentsLoaded(String dataSource)
  {
    Map<String, Double> status = getLoadStatus();
    if (status.containsKey(dataSource) && (status.get(dataSource)).doubleValue() == 100.0) {
      return true;
    } else {
      return false;
    }
  }

  public void unloadSegmentsForDataSource(String dataSource, Interval interval)
  {
    killDataSource(dataSource, false, interval);
  }

  public void deleteSegmentsDataSource(String dataSource, Interval interval)
  {
    killDataSource(dataSource, true, interval);
  }

  private void killDataSource(String dataSource, boolean kill, Interval interval)
  {
    try {
      makeRequest(
          HttpMethod.DELETE,
          String.format(
              "%sdatasources/%s?kill=%s&interval=%s",
              getCoordinatorURL(),
              dataSource, kill, URLEncoder.encode(interval.toString(), "UTF-8")
          )
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private StatusResponseHolder makeRequest(HttpMethod method, String url)
  {
    try {
      StatusResponseHolder response = new RequestBuilder(
          this.httpClient,
          method, new URL(url)
      )
          .go(responseHandler)
          .get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE("Error while making request to indexer [%s %s]", response.getStatus(), response.getContent());
      }
      return response;
    }
    catch (Exception e) {
      LOG.error(e, "Exception while sending request");
      throw Throwables.propagate(e);
    }
  }
}
