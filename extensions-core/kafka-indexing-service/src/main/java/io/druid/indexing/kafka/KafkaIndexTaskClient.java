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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.FullResponseHandler;
import com.metamx.http.client.response.FullResponseHolder;
import io.druid.indexing.common.RetryPolicy;
import io.druid.indexing.common.RetryPolicyConfig;
import io.druid.indexing.common.RetryPolicyFactory;
import io.druid.indexing.common.TaskInfoProvider;
import io.druid.indexing.common.TaskLocation;
import io.druid.indexing.common.TaskStatus;
import io.druid.segment.realtime.firehose.ChatHandlerResource;
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
  public class NoTaskLocationException extends RuntimeException
  {
    public NoTaskLocationException(String message) {
      super(message);
    }
  }

  public class TaskNotRunnableException extends RuntimeException
  {
    public TaskNotRunnableException(String message)
    {
      super(message);
    }
  }

  private static final EmittingLogger log = new EmittingLogger(KafkaIndexTaskClient.class);
  private static final String BASE_PATH = "/druid/worker/v1/chat";
  private static final int TASK_MISMATCH_RETRY_DELAY_SECONDS = 5;

  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;
  private final TaskInfoProvider taskInfoProvider;
  private final RetryPolicyFactory retryPolicyFactory;

  public KafkaIndexTaskClient(HttpClient httpClient, ObjectMapper jsonMapper, TaskInfoProvider taskInfoProvider)
  {
    this.httpClient = httpClient;
    this.jsonMapper = jsonMapper;
    this.taskInfoProvider = taskInfoProvider;
    this.retryPolicyFactory = createRetryPolicyFactory();
  }

  public void stop(String id, boolean publish)
  {
    submitRequest(id, HttpMethod.POST, "stop", publish ? "publish=true" : null, true);
  }

  public void resume(String id)
  {
    submitRequest(id, HttpMethod.POST, "resume", null, true);
  }

  public Map<Integer, Long> pause(String id)
  {
    return pause(id, 0);
  }

  public Map<Integer, Long> pause(String id, long timeout)
  {
    try {
      final FullResponseHolder response = submitRequest(
          id,
          HttpMethod.POST,
          "pause",
          timeout > 0 ? String.format("timeout=%d", timeout) : null,
          true
      );

      if (response.getStatus().equals(HttpResponseStatus.OK)) {
        return jsonMapper.readValue(response.getContent(), new TypeReference<Map<Integer, Long>>() {});
      }

      final RetryPolicy retryPolicy = retryPolicyFactory.makeRetryPolicy();
      while (true) {
        if (getStatus(id) == KafkaIndexTask.Status.PAUSED) {
          return getCurrentOffsets(id, true);
        }

        final Duration delay = retryPolicy.getAndIncrementRetryDelay();
        if (delay == null) {
          throw new ISE("Task [%s] failed to pause, aborting", id);
        } else {
          final long sleepTime = delay.getMillis();
          log.info(
              "Still waiting for task [%s] to pause; will try again in [%s]", id, new Duration(sleepTime).toString()
          );
          Thread.sleep(sleepTime);
        }
      }
    }
    catch (IOException | InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  public KafkaIndexTask.Status getStatus(String id)
  {
    try {
      final FullResponseHolder response = submitRequest(id, HttpMethod.GET, "status", null, true);
      return jsonMapper.readValue(response.getContent(), KafkaIndexTask.Status.class);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public DateTime getStartTime(String id, boolean retry)
  {
    try {
      final FullResponseHolder response = submitRequest(id, HttpMethod.GET, "time/start", null, retry);
      return response.getContent() == null || response.getContent().isEmpty()
             ? null
             : jsonMapper.readValue(response.getContent(), DateTime.class);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public Map<Integer, Long> getCurrentOffsets(String id, boolean retry)
  {
    try {
      final FullResponseHolder response = submitRequest(id, HttpMethod.GET, "offsets/current", null, retry);
      return jsonMapper.readValue(response.getContent(), new TypeReference<Map<Integer, Long>>() {});
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public Map<Integer, Long> getEndOffsets(String id)
  {
    try {
      final FullResponseHolder response = submitRequest(id, HttpMethod.GET, "offsets/end", null, true);
      return jsonMapper.readValue(response.getContent(), new TypeReference<Map<Integer, Long>>() {});
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public void setEndOffsets(String id, Map<Integer, Long> endOffsets)
  {
    setEndOffsets(id, endOffsets, false);
  }

  public void setEndOffsets(String id, Map<Integer, Long> endOffsets, boolean resume)
  {
    try {
      submitRequest(
          id,
          HttpMethod.POST,
          "offsets/end",
          resume ? "resume=true" : null,
          jsonMapper.writeValueAsBytes(endOffsets),
          true
      );
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @VisibleForTesting
  RetryPolicyFactory createRetryPolicyFactory()
  {
    // Retries for about a minute before giving up; this should be long enough to handle any temporary unresponsiveness
    // such as network issues, if a task is still in the process of starting up, or if the task is in the middle of
    // persisting to disk and doesn't respond immediately.

    return new RetryPolicyFactory(
        new RetryPolicyConfig()
            .setMinWait(Period.seconds(2))
            .setMaxWait(Period.seconds(8))
            .setMaxRetryCount(8)
    );
  }

  @VisibleForTesting
  void checkConnection(String host, int port) throws IOException
  {
    new Socket(host, port).close();
  }

  private FullResponseHolder submitRequest(String id, HttpMethod method, String pathSuffix, String query, boolean retry)
  {
    return submitRequest(id, method, pathSuffix, query, new byte[0], retry);
  }

  private FullResponseHolder submitRequest(
      String id,
      HttpMethod method,
      String pathSuffix,
      String query,
      byte[] content,
      boolean retry
  )
  {
    final RetryPolicy retryPolicy = retryPolicyFactory.makeRetryPolicy();
    while (true) {
      FullResponseHolder response = null;
      Request request = null;

      Optional<TaskStatus> status = taskInfoProvider.getTaskStatus(id);
      if (!status.isPresent() || !status.get().isRunnable()) {
        throw new TaskNotRunnableException(String.format("Aborting request because task [%s] is not runnable", id));
      }

      try {
        TaskLocation location = taskInfoProvider.getTaskLocation(id);
        if (location.equals(TaskLocation.unknown())) {
          log.info("No TaskLocation available for task [%s], this task may not have been assigned to a worker yet", id);
          throw new NoTaskLocationException(String.format("No TaskLocation available for task [%s]", id));
        }

        // Netty throws some annoying exceptions if a connection can't be opened, which happens relatively frequently
        // for tasks that happen to still be starting up, so test the connection first to keep the logs clean.
        checkConnection(location.getHost(), location.getPort());

        try {
          URI serviceUri = new URI(
              "http",
              null,
              location.getHost(),
              location.getPort(),
              String.format("%s/%s/%s", BASE_PATH, id, pathSuffix),
              query,
              null
          );
          request = new Request(method, serviceUri.toURL());

          // used to validate that we are talking to the correct worker
          request.addHeader(ChatHandlerResource.TASK_ID_HEADER, id);

          if (content.length > 0) {
            request.setContent(MediaType.APPLICATION_JSON, content);
          }

          response = httpClient.go(request, new FullResponseHandler(Charsets.UTF_8)).get();
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
          throw new IOException(String.format("Received status [%d]", responseCode));
        }
      }
      catch (IOException | ChannelException e) {

        // Since workers are free to move tasks around to different ports, there is a chance that a task may have been
        // moved but our view of its location has not been updated yet from ZK. To detect this case, we send a header
        // identifying our expected recipient in the request; if this doesn't correspond to the worker we messaged, the
        // worker will return an HTTP 404 with its ID in the response header. If we get a mismatching task ID, then
        // we will wait for a short period then retry the request indefinitely, expecting the task's location to
        // eventually be updated.

        final Duration delay;
        if (response != null && response.getStatus().equals(HttpResponseStatus.NOT_FOUND)) {
          String headerId = response.getResponse().headers().get(ChatHandlerResource.TASK_ID_HEADER);
          if (headerId != null && !headerId.equals(id)) {
            log.warn(
                "Expected worker to have taskId [%s] but has taskId [%s], will retry in [%d]s",
                id, headerId, TASK_MISMATCH_RETRY_DELAY_SECONDS
            );
            delay = Duration.standardSeconds(TASK_MISMATCH_RETRY_DELAY_SECONDS);
          } else {
            delay = retryPolicy.getAndIncrementRetryDelay();
          }
        } else {
          delay = retryPolicy.getAndIncrementRetryDelay();
        }

        if (!retry || delay == null) {
          Throwables.propagate(e);
        } else {
          try {
            final long sleepTime = delay.getMillis();
            log.debug(
                "Bad response HTTP [%d] from %s; will try again in [%s] (body: [%s])",
                (response != null ? response.getStatus().getCode() : 0),
                (request != null ? request.getUrl() : "-"),
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
