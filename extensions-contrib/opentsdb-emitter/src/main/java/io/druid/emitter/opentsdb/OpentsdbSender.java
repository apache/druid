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

package io.druid.emitter.opentsdb;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import io.druid.java.util.common.logger.Logger;

import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

public class OpentsdbSender
{
  /**
   * @see <a href="http://opentsdb.net/docs/build/html/api_http/put.html">Opentsdb - /api/put</a>
   */
  private static final String PATH = "/api/put";
  private static final Logger log = new Logger(OpentsdbSender.class);

  private final int batchSize;
  private final List<OpentsdbEvent> events;
  private final Client client;
  private final WebResource webResource;

  public OpentsdbSender(String host, int port, int connectionTimeout, int readTimeout, int batchSize)
  {
    this.batchSize = batchSize;
    events = new ArrayList<>(batchSize);

    client = Client.create();
    client.setConnectTimeout(connectionTimeout);
    client.setReadTimeout(readTimeout);
    webResource = client.resource("http://" + host + ":" + port + PATH);
  }

  public void send(OpentsdbEvent event)
  {
    events.add(event);
    if (events.size() >= batchSize) {
      sendEvents();
    }
  }

  public void flush()
  {
    sendEvents();
  }

  public void close()
  {
    flush();
    client.destroy();
  }

  private void sendEvents()
  {
    if (!events.isEmpty()) {
      try {
        webResource.entity(events, MediaType.APPLICATION_JSON_TYPE).post();
      }
      catch (Exception e) {
        log.error(e, "send to opentsdb server failed");
      }
      finally {
        events.clear();
      }
    }
  }

  @VisibleForTesting
  WebResource getWebResource()
  {
    return webResource;
  }
}
