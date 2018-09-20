/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.RetryPolicy;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClient;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.FullResponseHandler;
import org.apache.druid.java.util.http.client.response.FullResponseHolder;
import org.apache.druid.segment.realtime.firehose.ChatHandlerResource;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.util.Map;

public class KinesisIndexTaskClient extends SeekableStreamIndexTaskClient<String, String>
{
  public static class NoTaskLocationException extends RuntimeException
  {
    public NoTaskLocationException(String message)
    {
      super(message);
    }
  }

  public static class TaskNotRunnableException extends RuntimeException
  {
    public TaskNotRunnableException(String message)
    {
      super(message);
    }
  }

  private static ObjectMapper mapper = new ObjectMapper();
  public static final int MAX_RETRY_WAIT_SECONDS = 10;
  private static final int MIN_RETRY_WAIT_SECONDS = 2;
  private static final EmittingLogger log = new EmittingLogger(KinesisIndexTaskClient.class);
  private static final String BASE_PATH = "/druid/worker/v1/chat";
  private static final int TASK_MISMATCH_RETRY_DELAY_SECONDS = 5;

  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;
  private final TaskInfoProvider taskInfoProvider;
  private final Duration httpTimeout;
  private final RetryPolicyFactory retryPolicyFactory;
  private final ListeningExecutorService executorService;
  private final long numRetries;

  public KinesisIndexTaskClient(
      HttpClient httpClient,
      ObjectMapper jsonMapper,
      TaskInfoProvider taskInfoProvider,
      String dataSource,
      int numThreads,
      Duration httpTimeout,
      long numRetries
  )
  {
    super(
        httpClient,
        jsonMapper,
        taskInfoProvider,
        dataSource,
        numThreads,
        httpTimeout,
        numRetries
    );
    this.httpClient = httpClient;
    this.jsonMapper = jsonMapper;
    this.taskInfoProvider = taskInfoProvider;
    this.httpTimeout = httpTimeout;
    this.numRetries = numRetries;
    this.retryPolicyFactory = createRetryPolicyFactory();

    this.executorService = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(
            numThreads,
            String.format(
                "KinesisIndexTaskClient-%s-%%d",
                dataSource
            )
        )
    );
  }

  @Override
  protected JavaType constructMapType(Class<? extends Map> mapType)
  {
    return mapper.getTypeFactory().constructMapType(mapType, String.class, String.class);
  }

  @Override
  public void close()
  {
    executorService.shutdownNow();
  }


  @VisibleForTesting
  RetryPolicyFactory createRetryPolicyFactory()
  {
    // Retries [numRetries] times before giving up; this should be set long enough to handle any temporary
    // unresponsiveness such as network issues, if a task is still in the process of starting up, or if the task is in
    // the middle of persisting to disk and doesn't respond immediately.
    return new RetryPolicyFactory(
        new RetryPolicyConfig()
            .setMinWait(Period.seconds(MIN_RETRY_WAIT_SECONDS))
            .setMaxWait(Period.seconds(MAX_RETRY_WAIT_SECONDS))
            .setMaxRetryCount(numRetries)
    );
  }

  @Override
  @VisibleForTesting
  protected void checkConnection(String host, int port) throws IOException
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
      TaskLocation location = TaskLocation.unknown();
      String path = String.format("%s/%s/%s", BASE_PATH, id, pathSuffix);

      Optional<TaskStatus> status = taskInfoProvider.getTaskStatus(id);
      if (!status.isPresent() || !status.get().isRunnable()) {
        throw new TaskNotRunnableException(String.format("Aborting request because task [%s] is not runnable", id));
      }

      String host = location.getHost();
      String scheme = "";
      int port = -1;

      try {
        location = taskInfoProvider.getTaskLocation(id);
        if (location.equals(TaskLocation.unknown())) {
          throw new NoTaskLocationException(String.format("No TaskLocation available for task [%s]", id));
        }

        host = location.getHost();
        scheme = location.getTlsPort() >= 0 ? "https" : "http";
        port = location.getTlsPort() >= 0 ? location.getTlsPort() : location.getPort();

        // Netty throws some annoying exceptions if a connection can't be opened, which happens relatively frequently
        // for tasks that happen to still be starting up, so test the connection first to keep the logs clean.
        checkConnection(host, port);

        try {
          URI serviceUri = new URI(scheme, null, host, port, path, query, null);
          request = new Request(method, serviceUri.toURL());

          // used to validate that we are talking to the correct worker
          request.addHeader(ChatHandlerResource.TASK_ID_HEADER, id);

          if (content.length > 0) {
            request.setContent(MediaType.APPLICATION_JSON, content);
          }

          log.debug("HTTP %s: %s", method.getName(), serviceUri.toString());
          response = httpClient.go(request, new FullResponseHandler(Charsets.UTF_8), httpTimeout).get();
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

        String urlForLog = (request != null
                            ? request.getUrl().toString()
                            : StringUtils.format("%s://%s:%d%s", scheme, host, port, path));

        if (!retry) {
          // if retry=false, we probably aren't too concerned if the operation doesn't succeed (i.e. the request was
          // for informational purposes only) so don't log a scary stack trace
          log.info("submitRequest failed for [%s], with message [%s]", urlForLog, e.getMessage());
          Throwables.propagate(e);
        } else if (delay == null) {
          log.warn(e, "Retries exhausted for [%s], last exception:", urlForLog);
          Throwables.propagate(e);
        } else {
          try {
            final long sleepTime = delay.getMillis();
            log.debug(
                "Bad response HTTP [%s] from [%s]; will try again in [%s] (body/exception: [%s])",
                (response != null ? response.getStatus().getCode() : "no response"),
                urlForLog,
                new Duration(sleepTime).toString(),
                (response != null ? response.getContent() : e.getMessage())
            );
            Thread.sleep(sleepTime);
          }
          catch (InterruptedException e2) {
            Throwables.propagate(e2);
          }
        }
      }
      catch (NoTaskLocationException e) {
        log.info(
            "No TaskLocation available for task [%s], this task may not have been assigned to a worker yet or "
            + "may have already completed", id
        );
        throw e;
      }
      catch (Exception e) {
        log.warn(e, "Exception while sending request");
        throw e;
      }
    }
  }
}
