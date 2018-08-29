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

package org.apache.druid.indexing.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.IOE;
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

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;

/**
 * Abstract class to communicate with index tasks via HTTP. This class provides interfaces to serialize/deserialize
 * data and send an HTTP request.
 */
public abstract class IndexTaskClient implements AutoCloseable
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

  public static final int MAX_RETRY_WAIT_SECONDS = 10;

  private static final EmittingLogger log = new EmittingLogger(IndexTaskClient.class);
  private static final String BASE_PATH = "/druid/worker/v1/chat";
  private static final int MIN_RETRY_WAIT_SECONDS = 2;
  private static final int TASK_MISMATCH_RETRY_DELAY_SECONDS = 5;

  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final TaskInfoProvider taskInfoProvider;
  private final Duration httpTimeout;
  private final RetryPolicyFactory retryPolicyFactory;
  private final ListeningExecutorService executorService;

  public IndexTaskClient(
      HttpClient httpClient,
      ObjectMapper objectMapper,
      TaskInfoProvider taskInfoProvider,
      Duration httpTimeout,
      String callerId,
      int numThreads,
      long numRetries
  )
  {
    this.httpClient = httpClient;
    this.objectMapper = objectMapper;
    this.taskInfoProvider = taskInfoProvider;
    this.httpTimeout = httpTimeout;
    this.retryPolicyFactory = initializeRetryPolicyFactory(numRetries);
    this.executorService = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(
            numThreads,
            StringUtils.format(
                "IndexTaskClient-%s-%%d",
                callerId
            )
        )
    );
  }

  private static RetryPolicyFactory initializeRetryPolicyFactory(long numRetries)
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

  protected HttpClient getHttpClient()
  {
    return httpClient;
  }

  protected RetryPolicy newRetryPolicy()
  {
    return retryPolicyFactory.makeRetryPolicy();
  }

  protected <T> T deserialize(String content, TypeReference<T> typeReference) throws IOException
  {
    return objectMapper.readValue(content, typeReference);
  }

  protected <T> T deserialize(String content, Class<T> typeReference) throws IOException
  {
    return objectMapper.readValue(content, typeReference);
  }

  protected byte[] serialize(Object value) throws JsonProcessingException
  {
    return objectMapper.writeValueAsBytes(value);
  }

  protected <T> ListenableFuture<T> doAsync(Callable<T> callable)
  {
    return executorService.submit(callable);
  }

  protected boolean isSuccess(FullResponseHolder responseHolder)
  {
    return responseHolder.getStatus().getCode() / 100 == 2;
  }

  @VisibleForTesting
  protected void checkConnection(String host, int port) throws IOException
  {
    new Socket(host, port).close();
  }

  protected FullResponseHolder submitRequestWithEmptyContent(
      String taskId,
      HttpMethod method,
      String pathSuffix,
      @Nullable String query,
      boolean retry
  ) throws IOException, ChannelException, NoTaskLocationException
  {
    return submitRequest(taskId, null, method, pathSuffix, query, new byte[0], retry);
  }

  /**
   * To use this method, {@link #objectMapper} should be a jsonMapper.
   */
  protected FullResponseHolder submitJsonRequest(
      String taskId,
      HttpMethod method,
      String pathSuffix,
      @Nullable String query,
      byte[] content,
      boolean retry
  ) throws IOException, ChannelException, NoTaskLocationException
  {
    return submitRequest(taskId, MediaType.APPLICATION_JSON, method, pathSuffix, query, content, retry);
  }

  /**
   * To use this method, {@link #objectMapper} should be a smileMapper.
   */
  protected FullResponseHolder submitSmileRequest(
      String taskId,
      HttpMethod method,
      String pathSuffix,
      @Nullable String query,
      byte[] content,
      boolean retry
  ) throws IOException, ChannelException, NoTaskLocationException
  {
    return submitRequest(taskId, SmileMediaTypes.APPLICATION_JACKSON_SMILE, method, pathSuffix, query, content, retry);
  }

  /**
   * Sends an HTTP request to the task of the specified {@code taskId} and returns a response if it succeeded.
   */
  private FullResponseHolder submitRequest(
      String taskId,
      @Nullable String mediaType, // nullable if content is empty
      HttpMethod method,
      String pathSuffix,
      @Nullable String query,
      byte[] content,
      boolean retry
  ) throws IOException, ChannelException, NoTaskLocationException
  {
    final RetryPolicy retryPolicy = retryPolicyFactory.makeRetryPolicy();

    while (true) {
      FullResponseHolder response = null;
      Request request = null;
      TaskLocation location = TaskLocation.unknown();
      String path = StringUtils.format("%s/%s/%s", BASE_PATH, taskId, pathSuffix);

      Optional<TaskStatus> status = taskInfoProvider.getTaskStatus(taskId);
      if (!status.isPresent() || !status.get().isRunnable()) {
        throw new TaskNotRunnableException(StringUtils.format(
            "Aborting request because task [%s] is not runnable",
            taskId
        ));
      }

      String host = location.getHost();
      String scheme = "";
      int port = -1;

      try {
        location = taskInfoProvider.getTaskLocation(taskId);
        if (location.equals(TaskLocation.unknown())) {
          throw new NoTaskLocationException(StringUtils.format("No TaskLocation available for task [%s]", taskId));
        }

        host = location.getHost();
        scheme = location.getTlsPort() >= 0 ? "https" : "http";
        port = location.getTlsPort() >= 0 ? location.getTlsPort() : location.getPort();

        // Netty throws some annoying exceptions if a connection can't be opened, which happens relatively frequently
        // for tasks that happen to still be starting up, so test the connection first to keep the logs clean.
        checkConnection(host, port);

        try {
          URI serviceUri = new URI(
              scheme,
              null,
              host,
              port,
              path,
              query,
              null
          );
          request = new Request(method, serviceUri.toURL());

          // used to validate that we are talking to the correct worker
          request.addHeader(ChatHandlerResource.TASK_ID_HEADER, taskId);

          if (content.length > 0) {
            request.setContent(Preconditions.checkNotNull(mediaType, "mediaType"), content);
          }

          log.debug("HTTP %s: %s", method.getName(), serviceUri.toString());
          response = httpClient.go(request, new FullResponseHandler(StandardCharsets.UTF_8), httpTimeout).get();
        }
        catch (IOException | ChannelException ioce) {
          throw ioce;
        }
        catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(ie);
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }

        int responseCode = response.getStatus().getCode();
        if (responseCode / 100 == 2) {
          return response;
        } else if (responseCode == 400) { // don't bother retrying if it's a bad request
          throw new IAE("Received 400 Bad Request with body: %s", response.getContent());
        } else {
          throw new IOE("Received status [%d]", responseCode);
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
          if (headerId != null && !headerId.equals(taskId)) {
            log.warn(
                "Expected worker to have taskId [%s] but has taskId [%s], will retry in [%d]s",
                taskId, headerId, TASK_MISMATCH_RETRY_DELAY_SECONDS
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
                            : StringUtils.nonStrictFormat(
                                "%s://%s:%d%s",
                                scheme,
                                host,
                                port,
                                path
                            ));
        if (!retry) {
          // if retry=false, we probably aren't too concerned if the operation doesn't succeed (i.e. the request was
          // for informational purposes only) so don't log a scary stack trace
          log.info("submitRequest failed for [%s], with message [%s]", urlForLog, e.getMessage());
          throw e;
        } else if (delay == null) {
          log.warn(e, "Retries exhausted for [%s], last exception:", urlForLog);
          throw e;
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
            Thread.currentThread().interrupt();
            e.addSuppressed(e2);
            throw new RuntimeException(e);
          }
        }
      }
      catch (NoTaskLocationException e) {
        log.info("No TaskLocation available for task [%s], this task may not have been assigned to a worker yet or "
                 + "may have already completed", taskId);
        throw e;
      }
      catch (Exception e) {
        log.warn(e, "Exception while sending request");
        throw e;
      }
    }
  }

  @Override
  public void close()
  {
    executorService.shutdownNow();
  }
}
