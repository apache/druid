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

package io.druid.indexing.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.guice.annotations.Global;
import io.druid.indexing.common.RetryPolicy;
import io.druid.indexing.common.RetryPolicyConfig;
import io.druid.indexing.common.RetryPolicyFactory;
import io.druid.indexing.common.TaskLocation;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.util.Map;

public class KafkaIndexTaskClient
{
  private static final EmittingLogger log = new EmittingLogger(KafkaIndexTaskClient.class);
  private static final String BASE_PATH = "/druid/worker/v1/chat";

  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;
  private final RetryPolicyFactory retryPolicyFactory;

  @Inject
  public KafkaIndexTaskClient(@Global HttpClient httpClient, ObjectMapper jsonMapper)
  {
    this.httpClient = httpClient;
    this.jsonMapper = jsonMapper;
    this.retryPolicyFactory = new RetryPolicyFactory(
        new RetryPolicyConfig().setMinWait(Period.seconds(2))
                               .setMaxWait(Period.seconds(8))
                               .setMaxRetryCount(5)
    );
  }

  public void stop(TaskLocation location, String id, boolean publish)
  {
    try {
      final URI serviceUri = new URI(
          "http",
          null,
          location.getHost(),
          location.getPort(),
          String.format("%s/%s/%s", BASE_PATH, id, "stop"),
          publish ? "publish=true" : null,
          null
      );
      submitRequest(new Request(HttpMethod.POST, serviceUri.toURL()), true);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void resume(TaskLocation location, String id)
  {
    try {
      final URI serviceUri = new URI(
          "http",
          null,
          location.getHost(),
          location.getPort(),
          String.format("%s/%s/%s", BASE_PATH, id, "resume"),
          null,
          null
      );
      submitRequest(new Request(HttpMethod.POST, serviceUri.toURL()), true);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public Map<Integer, Long> pause(TaskLocation location, String id)
  {
    return pause(location, id, 0);
  }

  public Map<Integer, Long> pause(TaskLocation location, String id, long timeout)
  {
    try {
      final URI serviceUri = new URI(
          "http",
          null,
          location.getHost(),
          location.getPort(),
          String.format("%s/%s/%s", BASE_PATH, id, "pause"),
          timeout > 0 ? String.format("timeout=%d", timeout) : null,
          null
      );
      final StatusResponseHolder response = submitRequest(new Request(HttpMethod.POST, serviceUri.toURL()), true);

      if (response.getStatus().equals(HttpResponseStatus.OK)) {
        return jsonMapper.readValue(response.getContent(), new TypeReference<Map<Integer, Long>>() {});
      }

      final RetryPolicy retryPolicy = retryPolicyFactory.makeRetryPolicy();
      while (true) {
        if (getStatus(location, id) == KafkaIndexTask.Status.PAUSED) {
          return getCurrentOffsets(location, id, true);
        }

        final Duration delay = retryPolicy.getAndIncrementRetryDelay();
        if (delay == null) {
          throw new ISE("Task [%s] failed to pause, aborting", id);
        } else {
          final long sleepTime = delay.getMillis();
          log.info(
              "Still waiting for task [%s] to pause; will try again in [%s]",
              id,
              new Duration(sleepTime).toString()
          );
          Thread.sleep(sleepTime);
        }
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public KafkaIndexTask.Status getStatus(TaskLocation location, String id)
  {
    try {
      final URI serviceUri = new URI(
          "http",
          null,
          location.getHost(),
          location.getPort(),
          String.format("%s/%s/%s", BASE_PATH, id, "status"),
          null,
          null
      );
      final StatusResponseHolder response = submitRequest(new Request(HttpMethod.GET, serviceUri.toURL()), true);

      return jsonMapper.readValue(response.getContent(), KafkaIndexTask.Status.class);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public DateTime getStartTime(TaskLocation location, String id)
  {
    try {
      final URI serviceUri = new URI(
          "http",
          null,
          location.getHost(),
          location.getPort(),
          String.format("%s/%s/%s", BASE_PATH, id, "time/start"),
          null,
          null
      );
      final StatusResponseHolder response = submitRequest(new Request(HttpMethod.GET, serviceUri.toURL()), true);

      return response.getContent() == null || response.getContent().isEmpty()
             ? null
             : jsonMapper.readValue(response.getContent(), DateTime.class);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public Map<Integer, Long> getCurrentOffsets(TaskLocation location, String id, boolean retry)
  {
    try {
      final URI serviceUri = new URI(
          "http",
          null,
          location.getHost(),
          location.getPort(),
          String.format("%s/%s/%s", BASE_PATH, id, "offsets/current"),
          null,
          null
      );
      final StatusResponseHolder response = submitRequest(new Request(HttpMethod.GET, serviceUri.toURL()), retry);

      return jsonMapper.readValue(response.getContent(), new TypeReference<Map<Integer, Long>>() {});
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public Map<Integer, Long> getEndOffsets(TaskLocation location, String id)
  {
    try {
      final URI serviceUri = new URI(
          "http",
          null,
          location.getHost(),
          location.getPort(),
          String.format("%s/%s/%s", BASE_PATH, id, "offsets/end"),
          null,
          null
      );
      final StatusResponseHolder response = submitRequest(new Request(HttpMethod.GET, serviceUri.toURL()), true);

      return jsonMapper.readValue(response.getContent(), new TypeReference<Map<Integer, Long>>() {});
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void setEndOffsets(TaskLocation location, String id, Map<Integer, Long> endOffsets)
  {
    setEndOffsets(location, id, endOffsets, false);
  }

  public void setEndOffsets(TaskLocation location, String id, Map<Integer, Long> endOffsets, boolean resume)
  {
    try {
      final URI serviceUri = new URI(
          "http",
          null,
          location.getHost(),
          location.getPort(),
          String.format("%s/%s/%s", BASE_PATH, id, "offsets/end"),
          resume ? "resume=true" : null,
          null
      );
      submitRequest(
          new Request(HttpMethod.POST, serviceUri.toURL()).setContent(
              MediaType.APPLICATION_JSON,
              jsonMapper.writeValueAsBytes(endOffsets)
          ), true
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private StatusResponseHolder submitRequest(Request request, boolean retry)
  {
    final RetryPolicy retryPolicy = retryPolicyFactory.makeRetryPolicy();
    while (true) {
      StatusResponseHolder response = null;

      try {
        // Netty throws some annoying exceptions if a connection can't be opened, which happens relatively frequently
        // for tasks that happen to still be starting up, so test the connection first to keep the logs clean.
        new Socket(request.getUrl().getHost(), request.getUrl().getPort()).close();

        try {
          response = httpClient.go(request, new StatusResponseHandler(Charsets.UTF_8)).get();
        }
        catch (Exception e) {
          Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
          Throwables.propagateIfInstanceOf(e.getCause(), ChannelException.class);
          throw Throwables.propagate(e);
        }

        int responseCode = response.getStatus().getCode();
        if (responseCode / 100 == 2) {
          return response;
        } else if (responseCode == 400) { // don't bother retrying if it's a bad request
          throw new IAE("Received 400 Bad Request with body: %s", response.getContent());
        } else {
          throw new IOException(String.format("Received status [%d] with: %s", responseCode, response.getContent()));
        }
      }
      catch (IOException | ChannelException e) {
        final Duration delay = retryPolicy.getAndIncrementRetryDelay();
        if (!retry || delay == null) {
          Throwables.propagate(e);
        } else {
          try {
            final long sleepTime = delay.getMillis();
            log.debug(
                "Bad response HTTP [%d] from %s; will try again in [%s] (body: [%s])",
                (response != null ? response.getStatus().getCode() : 0),
                request.getUrl(),
                new Duration(sleepTime).toString(),
                (response != null ? response.getContent() : "[empty]")
            );
            Thread.sleep(sleepTime);
          }
          catch (InterruptedException e2) {
            Throwables.propagate(e2);
          }
        }
      }
    }
  }
}
