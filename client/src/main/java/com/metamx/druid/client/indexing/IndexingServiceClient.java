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

package com.metamx.druid.client.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.druid.client.DataSegment;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.InputStreamResponseHandler;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.joda.time.Interval;

import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.List;

public class IndexingServiceClient
{
  private static final InputStreamResponseHandler RESPONSE_HANDLER = new InputStreamResponseHandler();

  private final HttpClient client;
  private final ObjectMapper jsonMapper;
  private final ServiceProvider serviceProvider;

  public IndexingServiceClient(
      HttpClient client,
      ObjectMapper jsonMapper,
      ServiceProvider serviceProvider
  )
  {
    this.client = client;
    this.jsonMapper = jsonMapper;
    this.serviceProvider = serviceProvider;
  }

  public void mergeSegments(List<DataSegment> segments)
  {
    final Iterator<DataSegment> segmentsIter = segments.iterator();
    if (!segmentsIter.hasNext()) {
      return;
    }

    final String dataSource = segmentsIter.next().getDataSource();
    while (segmentsIter.hasNext()) {
      DataSegment next = segmentsIter.next();
      if (!dataSource.equals(next.getDataSource())) {
        throw new IAE("Cannot merge segments of different dataSources[%s] and [%s]", dataSource, next.getDataSource());
      }
    }

    runQuery("merge", new ClientAppendQuery(dataSource, segments));
  }

  public void killSegments(String dataSource, Interval interval)
  {
    runQuery("index", new ClientKillQuery(dataSource, interval));
  }

  public void upgradeSegment(DataSegment dataSegment)
  {
    runQuery("task", new ClientConversionQuery(dataSegment));
  }

  public void upgradeSegments(String dataSource, Interval interval)
  {
    runQuery("task", new ClientConversionQuery(dataSource, interval));
  }

  private InputStream runQuery(String endpoint, Object queryObject)
  {
    try {
      return client.post(new URL(String.format("%s/%s", baseUrl(), endpoint)))
                   .setContent("application/json", jsonMapper.writeValueAsBytes(queryObject))
                   .go(RESPONSE_HANDLER)
                   .get();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private String baseUrl()
  {
    try {
      final ServiceInstance instance = serviceProvider.getInstance();
      if (instance == null) {
        throw new ISE("Cannot find instance of indexingService");
      }

      return String.format("http://%s:%s/druid/indexer/v1", instance.getAddress(), instance.getPort());
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
