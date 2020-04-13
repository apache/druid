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
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
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
import org.apache.druid.java.util.http.client.response.StringFullResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.segment.realtime.firehose.ChatHandlerResource;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

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

  protected <T> T deserialize(String content, JavaType type) throws IOException
  {
    return objectMapper.readValue(content, type);
  }

  protected <T> T deserialize(String content, TypeReference<T> typeReference) throws IOException
  {
    return objectMapper.readValue(content, typeReference);
  }

  protected <T> T deserialize(String content, Class<T> typeReference) throws IOException
  {
    return objectMapper.readValue(content, typeReference);
  }

  protected <T> T deserializeMap(String content, Class<? extends Map> mapClass, Class<?> keyClass, Class<?> valueClass)
      throws IOException
  {
    return deserialize(content, objectMapper.getTypeFactory().constructMapType(mapClass, keyClass, valueClass));
  }

  protected <T> T deserializeNestedValueMap(
      String content,
      Class<? extends Map> mapClass,
      Class<?> keyClass,
      Class<? extends Map> valueMapClass,
      Class<?> valueMapClassKey,
      Class<?> valueMapClassValue
  )
      throws IOException
  {
    TypeFactory factory = objectMapper.getTypeFactory();
    return deserialize(
        content,
        factory.constructMapType(
            mapClass,
            factory.constructType(keyClass),
            factory.constructMapType(valueMapClass, valueMapClassKey, valueMapClassValue)
        )
    );
  }

  protected byte[] serialize(Object value) throws JsonProcessingException
  {
    return objectMapper.writeValueAsBytes(value);
  }

  protected <T> ListenableFuture<T> doAsync(Callable<T> callable)
  {
    return executorService.submit(callable);
  }

  protected boolean isSuccess(StringFullResponseHolder responseHolder)
  {
    return responseHolder.getStatus().getCode() / 100 == 2;
  }

  @VisibleForTesting
  protected void checkConnection(String host, int port) throws IOException
  {
    new Socket(host, port).close();
  }

  protected StringFullResponseHolder submitRequestWithEmptyContent(
      String taskId,
      HttpMethod method,
      String encodedPathSuffix,
      @Nullable String encodedQueryString,
      boolean retry
  ) throws IOException, ChannelException, NoTaskLocationException
  {
    return submitRequest(taskId, null, method, encodedPathSuffix, encodedQueryString, new byte[0], retry);
  }

  /**
   * To use this method, {@link #objectMapper} should be a jsonMapper.
   */
  protected StringFullResponseHolder submitJsonRequest(
      String taskId,
      HttpMethod method,
      String encodedPathSuffix,
      @Nullable String encodedQueryString,
      byte[] content,
      boolean retry
  ) throws IOException, ChannelException, NoTaskLocationException
  {
    return submitRequest(
        taskId,
        MediaType.APPLICATION_JSON,
        method,
        encodedPathSuffix,
        encodedQueryString,
        content,
        retry
    );
  }

  /**
   * To use this method, {@link #objectMapper} should be a smileMapper.
   */
  protected StringFullResponseHolder submitSmileRequest(
      String taskId,
      HttpMethod method,
      String encodedPathSuffix,
      @Nullable String encodedQueryString,
      byte[] content,
      boolean retry
  ) throws IOException, ChannelException, NoTaskLocationException
  {
    return submitRequest(
        taskId,
        SmileMediaTypes.APPLICATION_JACKSON_SMILE,
        method,
        encodedPathSuffix,
        encodedQueryString,
        content,
        retry
    );
  }

  private Request createRequest(
      String taskId,
      TaskLocation location,
      String path,
      @Nullable String encodedQueryString,
      HttpMethod method,
      @Nullable String mediaType,
      byte[] content
  ) throws MalformedURLException
  {
    final String host = location.getHost();
    final String scheme = location.getTlsPort() >= 0 ? "https" : "http";
    final int port = location.getTlsPort() >= 0 ? location.getTlsPort() : location.getPort();

    // Use URL constructor, not URI, since the path is already encoded.
    // The below line can throw a MalformedURLException, and this method should return immediately without rety.
    final URL serviceUrl = new URL(
        scheme,
        host,
        port,
        encodedQueryString == null ? path : StringUtils.format("%s?%s", path, encodedQueryString)
    );

    final Request request = new Request(method, serviceUrl);
    // used to validate that we are talking to the correct worker
    request.addHeader(ChatHandlerResource.TASK_ID_HEADER, StringUtils.urlEncode(taskId));
    if (content.length > 0) {
      request.setContent(Preconditions.checkNotNull(mediaType, "mediaType"), content);
    }
    return request;
  }

  /**
   * Sends an HTTP request to the task of the specified {@code taskId} and returns a response if it succeeded.
   */
  private StringFullResponseHolder submitRequest(
      String taskId,
      @Nullable String mediaType, // nullable if content is empty
      HttpMethod method,
      String encodedPathSuffix,
      @Nullable String encodedQueryString,
      byte[] content,
      boolean retry
  ) throws IOException, ChannelException, NoTaskLocationException
  {
    final RetryPolicy retryPolicy = retryPolicyFactory.makeRetryPolicy();

    while (true) {
      String path = StringUtils.format("%s/%s/%s", BASE_PATH, StringUtils.urlEncode(taskId), encodedPathSuffix);

      Optional<TaskStatus> status = taskInfoProvider.getTaskStatus(taskId);
      if (!status.isPresent() || !status.get().isRunnable()) {
        throw new TaskNotRunnableException(
            StringUtils.format(
                "Aborting request because task [%s] is not runnable",
                taskId
            )
        );
      }

      final TaskLocation location = taskInfoProvider.getTaskLocation(taskId);
      if (location.equals(TaskLocation.unknown())) {
        throw new NoTaskLocationException(StringUtils.format("No TaskLocation available for task [%s]", taskId));
      }

      final Request request = createRequest(
          taskId,
          location,
          path,
          encodedQueryString,
          method,
          mediaType,
          content
      );

      StringFullResponseHolder response = null;
      try {
        // Netty throws some annoying exceptions if a connection can't be opened, which happens relatively frequently
        // for tasks that happen to still be starting up, so test the connection first to keep the logs clean.
        checkConnection(request.getUrl().getHost(), request.getUrl().getPort());

        response = submitRequest(request);

        int responseCode = response.getStatus().getCode();
        if (responseCode / 100 == 2) {
          return response;
        } else if (responseCode == 400) { // don't bother retrying if it's a bad request
          throw new IAE("Received 400 Bad Request with body: %s", response.getContent());
        } else {
          throw new IOE("Received status [%d] and content [%s]", responseCode, response.getContent());
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
          String headerId = StringUtils.urlDecode(
              response.getResponse().headers().get(ChatHandlerResource.TASK_ID_HEADER)
          );
          if (headerId != null && !headerId.equals(taskId)) {
            log.warn(
                "Expected worker to have taskId [%s] but has taskId [%s], will retry in [%d]s",
                taskId,
                headerId,
                TASK_MISMATCH_RETRY_DELAY_SECONDS
            );
            delay = Duration.standardSeconds(TASK_MISMATCH_RETRY_DELAY_SECONDS);
          } else {
            delay = retryPolicy.getAndIncrementRetryDelay();
          }
        } else {
          delay = retryPolicy.getAndIncrementRetryDelay();
        }
        final String urlForLog = request.getUrl().toString();
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
        log.info(
            "No TaskLocation available for task [%s], this task may not have been assigned to a worker yet "
            + "or may have already completed",
            taskId
        );
        throw e;
      }
      catch (Exception e) {
        log.warn(e, "Exception while sending request");
        throw e;
      }
    }
  }

  private StringFullResponseHolder submitRequest(Request request) throws IOException, ChannelException
  {
    try {
      log.debug("HTTP %s: %s", request.getMethod().getName(), request.getUrl().toString());
      return httpClient.go(request, new StringFullResponseHandler(StandardCharsets.UTF_8), httpTimeout).get();
    }
    catch (Exception e) {
      throw throwIfPossible(e);
    }
  }

  /**
   * Throws if it's possible to throw the given Throwable.
   * <p>
   * - The input throwable shouldn't be null.
   * - If Throwable is an {@link ExecutionException}, this calls itself recursively with the cause of ExecutionException.
   * - If Throwable is an {@link IOException} or a {@link ChannelException}, this simply throws it.
   * - If Throwable is an {@link InterruptedException}, this interrupts the current thread and throws a RuntimeException
   * wrapping the InterruptedException
   * - Otherwise, this simply returns the given Throwable.
   * <p>
   * Note that if the given Throable is an ExecutionException, this can return the cause of ExecutionException.
   */
  private RuntimeException throwIfPossible(Throwable t) throws IOException, ChannelException
  {
    Preconditions.checkNotNull(t, "Throwable shoulnd't null");

    if (t instanceof ExecutionException) {
      if (t.getCause() != null) {
        return throwIfPossible(t.getCause());
      } else {
        return new RuntimeException(t);
      }
    }

    if (t instanceof IOException) {
      throw (IOException) t;
    }

    if (t instanceof ChannelException) {
      throw (ChannelException) t;
    }

    if (t instanceof InterruptedException) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(t);
    }

    Throwables.propagateIfPossible(t);
    return new RuntimeException(t);
  }

  @Override
  public void close()
  {
    executorService.shutdownNow();
  }
}
