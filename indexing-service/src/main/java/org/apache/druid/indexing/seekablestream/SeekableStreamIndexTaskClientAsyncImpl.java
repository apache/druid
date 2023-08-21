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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.RetryPolicy;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.IgnoreHttpResponseHandler;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceClosedException;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.ServiceLocations;
import org.apache.druid.rpc.ServiceLocator;
import org.apache.druid.rpc.ServiceNotAvailableException;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.rpc.indexing.SpecificTaskRetryPolicy;
import org.apache.druid.segment.incremental.ParseExceptionReport;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Implementation of {@link SeekableStreamIndexTaskClient} based on {@link ServiceClient}.
 */
public abstract class SeekableStreamIndexTaskClientAsyncImpl<PartitionIdType, SequenceOffsetType>
    implements SeekableStreamIndexTaskClient<PartitionIdType, SequenceOffsetType>
{
  private static final EmittingLogger log = new EmittingLogger(SeekableStreamIndexTaskClientAsyncImpl.class);

  public static final int MIN_RETRY_WAIT_SECONDS = 2;
  public static final int MAX_RETRY_WAIT_SECONDS = 10;

  private final ServiceClientFactory serviceClientFactory;
  private final TaskInfoProvider taskInfoProvider;
  private final ObjectMapper jsonMapper;
  private final Duration httpTimeout;
  private final long httpRetries;

  // Used by getOffsetsWhenPaused, due to special retry logic.
  private final ScheduledExecutorService retryExec;

  public SeekableStreamIndexTaskClientAsyncImpl(
      final String dataSource,
      final ServiceClientFactory serviceClientFactory,
      final TaskInfoProvider taskInfoProvider,
      final ObjectMapper jsonMapper,
      final Duration httpTimeout,
      final long httpRetries
  )
  {
    this.serviceClientFactory = serviceClientFactory;
    this.taskInfoProvider = taskInfoProvider;
    this.jsonMapper = jsonMapper;
    this.httpTimeout = httpTimeout;
    this.httpRetries = httpRetries;
    this.retryExec = Execs.scheduledSingleThreaded(
        StringUtils.format(
            "%s-%s-%%d",
            getClass().getSimpleName(),
            StringUtils.encodeForFormat(dataSource)
        )
    );
  }

  @Override
  @SuppressWarnings("unchecked")
  public ListenableFuture<TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>>> getCheckpointsAsync(
      final String id,
      final boolean retry
  )
  {
    return makeRequest(id, new RequestBuilder(HttpMethod.GET, "/checkpoints"))
        .handler(new BytesFullResponseHandler())
        .onSuccess(r -> {
          final TypeFactory factory = jsonMapper.getTypeFactory();
          return (TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>>)
              JacksonUtils.readValue(
                  jsonMapper,
                  r.getContent(),
                  factory.constructMapType(
                      TreeMap.class,
                      factory.constructType(Integer.class),
                      factory.constructMapType(Map.class, getPartitionType(), getSequenceType())
                  )
              );
        })
        .onNotAvailable(e -> Either.value(new TreeMap<>()))
        .retry(retry)
        .go();
  }

  @Override
  public ListenableFuture<Boolean> stopAsync(final String id, final boolean publish)
  {
    return makeRequest(id, new RequestBuilder(HttpMethod.POST, "/stop" + (publish ? "?publish=true" : "")))
        .onSuccess(r -> true)
        .onHttpError(e -> {
          log.warn("Task [%s] coundln't be stopped because of http request failure [%s].", id, e.getMessage());
          return Either.value(false);
        })
        .onNotAvailable(e -> {
          log.warn("Task [%s] coundln't be stopped because it is not available.", id);
          return Either.value(false);
        })
        .onClosed(e -> {
          log.warn("Task [%s] couldn't be stopped because it is no longer running.", id);
          return Either.value(true);
        })
        .go();
  }

  @Override
  public ListenableFuture<Boolean> resumeAsync(final String id)
  {
    return makeRequest(id, new RequestBuilder(HttpMethod.POST, "/resume"))
        .onSuccess(r -> true)
        .onException(e -> Either.value(false))
        .go();
  }

  @Override
  public ListenableFuture<Map<PartitionIdType, SequenceOffsetType>> getCurrentOffsetsAsync(String id, boolean retry)
  {
    return makeRequest(id, new RequestBuilder(HttpMethod.GET, "/offsets/current"))
        .handler(new BytesFullResponseHandler())
        .onSuccess(r -> deserializeOffsetsMap(r.getContent()))
        .onNotAvailable(e -> Either.value(Collections.emptyMap()))
        .retry(retry)
        .go();
  }

  @Override
  public ListenableFuture<Map<PartitionIdType, SequenceOffsetType>> getEndOffsetsAsync(String id)
  {
    return makeRequest(id, new RequestBuilder(HttpMethod.GET, "/offsets/end"))
        .handler(new BytesFullResponseHandler())
        .onSuccess(r -> deserializeOffsetsMap(r.getContent()))
        .onNotAvailable(e -> Either.value(Collections.emptyMap()))
        .go();
  }

  @Override
  public ListenableFuture<Boolean> setEndOffsetsAsync(
      final String id,
      final Map<PartitionIdType, SequenceOffsetType> endOffsets,
      final boolean finalize
  )
  {
    final RequestBuilder requestBuilder = new RequestBuilder(
        HttpMethod.POST,
        StringUtils.format("/offsets/end?finish=%s", finalize)
    ).jsonContent(jsonMapper, endOffsets);

    return makeRequest(id, requestBuilder)
        .handler(IgnoreHttpResponseHandler.INSTANCE)
        .onSuccess(r -> true)
        .go();
  }

  @Override
  public ListenableFuture<SeekableStreamIndexTaskRunner.Status> getStatusAsync(final String id)
  {
    return makeRequest(id, new RequestBuilder(HttpMethod.GET, "/status"))
        .handler(new BytesFullResponseHandler())
        .onSuccess(
            r ->
                JacksonUtils.readValue(jsonMapper, r.getContent(), SeekableStreamIndexTaskRunner.Status.class)
        )
        .onNotAvailable(e -> Either.value(SeekableStreamIndexTaskRunner.Status.NOT_STARTED))
        .go();
  }

  @Override
  public ListenableFuture<DateTime> getStartTimeAsync(String id)
  {
    return makeRequest(id, new RequestBuilder(HttpMethod.GET, "/time/start"))
        .handler(new BytesFullResponseHandler())
        .onSuccess(r -> {
          if (isNullOrEmpty(r.getContent())) {
            return null;
          } else {
            return JacksonUtils.readValue(jsonMapper, r.getContent(), DateTime.class);
          }
        })
        .onNotAvailable(e -> Either.value(null))
        .go();
  }

  @Override
  public ListenableFuture<Map<PartitionIdType, SequenceOffsetType>> pauseAsync(String id)
  {
    final ListenableFuture<Map<PartitionIdType, SequenceOffsetType>> pauseFuture =
        makeRequest(id, new RequestBuilder(HttpMethod.POST, "/pause"))
            .handler(new BytesFullResponseHandler())
            .onSuccess(r -> {
              if (r.getStatus().equals(HttpResponseStatus.OK)) {
                log.info("Task [%s] paused successfully", id);
                return deserializeOffsetsMap(r.getContent());
              } else if (r.getStatus().equals(HttpResponseStatus.ACCEPTED)) {
                // Return null, which triggers a loop later to wait for the task to enter PAUSED state.
                return null;
              } else {
                throw new ISE(
                    "Pause request for task [%s] failed with response [%s]",
                    id,
                    r.getStatus()
                );
              }
            })
            .onNotAvailable(e -> Either.value(Collections.emptyMap()))
            .go();

    return FutureUtils.transformAsync(
        pauseFuture,
        result -> {
          if (result != null) {
            return Futures.immediateFuture(result);
          } else {
            return getOffsetsWhenPaused(
                id,
                new RetryPolicyFactory(
                    new RetryPolicyConfig()
                        .setMinWait(Period.seconds(MIN_RETRY_WAIT_SECONDS))
                        .setMaxWait(Period.seconds(MAX_RETRY_WAIT_SECONDS))
                        .setMaxRetryCount(httpRetries)
                ).makeRetryPolicy()
            );
          }
        }
    );
  }

  @Override
  public ListenableFuture<Map<String, Object>> getMovingAveragesAsync(String id)
  {
    return makeRequest(id, new RequestBuilder(HttpMethod.GET, "/rowStats"))
        .handler(new BytesFullResponseHandler())
        .onSuccess(r -> {
          if (isNullOrEmpty(r.getContent())) {
            log.warn("Got empty response when calling getMovingAverages, id[%s]", id);
            return null;
          } else {
            return JacksonUtils.readValue(jsonMapper, r.getContent(), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);
          }
        })
        .onNotAvailable(e -> Either.value(Collections.emptyMap()))
        .go();
  }

  @Override
  public ListenableFuture<List<ParseExceptionReport>> getParseErrorsAsync(String id)
  {
    return makeRequest(id, new RequestBuilder(HttpMethod.GET, "/unparseableEvents"))
        .handler(new BytesFullResponseHandler())
        .onSuccess(r -> {
          if (isNullOrEmpty(r.getContent())) {
            log.warn("Got empty response when calling getParseErrors, id[%s]", id);
            return null;
          } else {
            return JacksonUtils.readValue(
                jsonMapper,
                r.getContent(),
                TYPE_REFERENCE_LIST_PARSE_EXCEPTION_REPORT
            );
          }
        })
        .onNotAvailable(e -> Either.value(Collections.emptyList()))
        .go();
  }

  @Override
  public void close()
  {
    retryExec.shutdownNow();
  }

  /**
   * Create a {@link SeekableStreamRequestBuilder}.
   */
  private SeekableStreamRequestBuilder<Void, Void, Void> makeRequest(
      String taskId,
      RequestBuilder requestBuilder
  )
  {
    return new SeekableStreamRequestBuilder<>(
        taskId,
        requestBuilder,
        IgnoreHttpResponseHandler.INSTANCE,
        Function.identity()
    );
  }

  /**
   * Helper for deserializing offset maps.
   */
  private Map<PartitionIdType, SequenceOffsetType> deserializeOffsetsMap(final byte[] content)
  {
    final MapType offsetsMapType =
        jsonMapper.getTypeFactory().constructMapType(Map.class, getPartitionType(), getSequenceType());
    return JacksonUtils.readValue(jsonMapper, content, offsetsMapType);
  }

  /**
   * Helper for {@link #pauseAsync}.
   *
   * Calls {@link #getStatusAsync} in a loop until a task is paused, then calls {@link #getCurrentOffsetsAsync} to
   * get the post-pause offsets for the task.
   */
  private ListenableFuture<Map<PartitionIdType, SequenceOffsetType>> getOffsetsWhenPaused(
      final String taskId,
      final RetryPolicy retryPolicy
  )
  {
    final ListenableFuture<SeekableStreamIndexTaskRunner.Status> statusFuture = getStatusAsync(taskId);

    return FutureUtils.transformAsync(
        statusFuture,
        status -> {
          if (status == SeekableStreamIndexTaskRunner.Status.PAUSED) {
            return getCurrentOffsetsAsync(taskId, true);
          } else {
            final Duration delay;

            synchronized (retryPolicy) {
              delay = retryPolicy.getAndIncrementRetryDelay();
            }

            if (delay == null) {
              return Futures.immediateFailedFuture(
                  new ISE(
                      "Task [%s] failed to change its status from [%s] to [%s], aborting",
                      taskId,
                      status,
                      SeekableStreamIndexTaskRunner.Status.PAUSED
                  )
              );
            } else {
              final long sleepTime = delay.getMillis();
              final SettableFuture<Map<PartitionIdType, SequenceOffsetType>> retVal = SettableFuture.create();
              retryExec.schedule(
                  () ->
                      Futures.addCallback(
                          getOffsetsWhenPaused(taskId, retryPolicy),
                          new FutureCallback<Map<PartitionIdType, SequenceOffsetType>>()
                          {
                            @Override
                            public void onSuccess(@Nullable Map<PartitionIdType, SequenceOffsetType> result)
                            {
                              retVal.set(result);
                            }

                            @Override
                            public void onFailure(Throwable t)
                            {
                              retVal.setException(t);
                            }
                          }
                      ),
                  sleepTime,
                  TimeUnit.MILLISECONDS
              );

              return retVal;
            }
          }
        }
    );
  }

  private static boolean isNullOrEmpty(@Nullable final byte[] content)
  {
    return content == null || content.length == 0;
  }

  /**
   * Helper for setting up each request's desired response, error handling, and retry behavior.
   */
  private class SeekableStreamRequestBuilder<IntermediateType, FinalType, T>
  {
    private final String taskId;
    private final RequestBuilder requestBuilder;

    private final List<Function<Throwable, Either<Throwable, T>>> exceptionMappers = new ArrayList<>();
    private HttpResponseHandler<IntermediateType, FinalType> responseHandler;
    private Function<FinalType, T> responseTransformer;
    private boolean retry = true;

    SeekableStreamRequestBuilder(
        String taskId,
        RequestBuilder requestBuilder,
        HttpResponseHandler<IntermediateType, FinalType> responseHandler,
        Function<FinalType, T> responseTransformer
    )
    {
      this.taskId = taskId;
      this.requestBuilder = requestBuilder;
      this.responseHandler = responseHandler;
      this.responseTransformer = responseTransformer;
    }

    /**
     * Handler for requests. The result from this handler is fed into the transformer provided by {@link #onSuccess}.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <NewIntermediateType, NewFinalType> SeekableStreamRequestBuilder<NewIntermediateType, NewFinalType, T> handler(
        final HttpResponseHandler<NewIntermediateType, NewFinalType> handler
    )
    {
      this.responseHandler = (HttpResponseHandler) handler;
      return (SeekableStreamRequestBuilder) this;
    }

    /**
     * Response mapping for successful requests.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <NewT> SeekableStreamRequestBuilder<IntermediateType, FinalType, NewT> onSuccess(
        final Function<FinalType, NewT> responseTransformer
    )
    {
      this.responseTransformer = (Function) responseTransformer;
      return (SeekableStreamRequestBuilder) this;
    }

    /**
     * Whether the request should be retried on failure. Default is true.
     */
    public SeekableStreamRequestBuilder<IntermediateType, FinalType, T> retry(boolean retry)
    {
      this.retry = retry;
      return this;
    }

    /**
     * Error mapping for all exceptions.
     */
    public SeekableStreamRequestBuilder<IntermediateType, FinalType, T> onException(final Function<Throwable, Either<Throwable, T>> fn)
    {
      exceptionMappers.add(fn);
      return this;
    }

    /**
     * Error mapping for {@link HttpResponseException}, which occurs when a task returns a non-2xx HTTP code.
     */
    public SeekableStreamRequestBuilder<IntermediateType, FinalType, T> onHttpError(final Function<HttpResponseException, Either<Throwable, T>> fn)
    {
      return onException(e -> {
        if (e instanceof HttpResponseException) {
          return fn.apply((HttpResponseException) e);
        } else {
          return Either.error(e);
        }
      });
    }

    /**
     * Error mapping for {@link ServiceNotAvailableException}, which occurs when a task is not available.
     */
    public SeekableStreamRequestBuilder<IntermediateType, FinalType, T> onNotAvailable(final Function<ServiceNotAvailableException, Either<Throwable, T>> fn)
    {
      return onException(e -> {
        if (e instanceof ServiceNotAvailableException) {
          return fn.apply((ServiceNotAvailableException) e);
        } else {
          return Either.error(e);
        }
      });
    }

    /**
     * Error mapping for {@link ServiceClosedException}, which occurs when a task is not running.
     */
    public SeekableStreamRequestBuilder<IntermediateType, FinalType, T> onClosed(final Function<ServiceClosedException, Either<Throwable, T>> fn)
    {
      return onException(e -> {
        if (e instanceof ServiceClosedException) {
          return fn.apply((ServiceClosedException) e);
        } else {
          return Either.error(e);
        }
      });
    }

    /**
     * Issue the request.
     */
    public ListenableFuture<T> go()
    {
      final ServiceClient client = makeClient(taskId, retry);
      final SettableFuture<T> retVal = SettableFuture.create();

      Futures.addCallback(
          FutureUtils.transform(
              client.asyncRequest(requestBuilder.timeout(httpTimeout), responseHandler),
              responseTransformer
          ),
          new FutureCallback<T>()
          {
            @Override
            public void onSuccess(@Nullable T result)
            {
              retVal.set(result);
            }

            @Override
            public void onFailure(Throwable t)
            {
              Either<Throwable, T> either = Either.error(t);

              for (final Function<Throwable, Either<Throwable, T>> exceptionMapper : exceptionMappers) {
                if (!either.isError()) {
                  break;
                }

                try {
                  final Either<Throwable, T> nextEither = exceptionMapper.apply(either.error());
                  if (nextEither != null) {
                    either = nextEither;
                  }
                }
                catch (Throwable e) {
                  // Not expected: on-error function should never throw exceptions. Continue mapping.
                  log.warn(e, "Failed to map exception encountered while contacting task [%s]", taskId);
                }
              }

              if (either.isError()) {
                retVal.setException(either.error());
              } else {
                retVal.set(either.valueOrThrow());
              }
            }
          }
      );

      return retVal;
    }

    private ServiceClient makeClient(final String taskId, final boolean retry)
    {
      final ServiceRetryPolicy retryPolicy = makeRetryPolicy(taskId, retry);

      // We're creating a new locator for each request and not closing it. This is OK, since SeekableStreamTaskLocator
      // is stateless, cheap to create, and its close() method does nothing.
      final SeekableStreamTaskLocator locator = new SeekableStreamTaskLocator(taskInfoProvider, taskId);

      // We're creating a new client for each request. This is OK, clients are cheap to create and do not contain
      // state that is important for us to retain across requests. (The main state they retain is preferred location
      // from prior redirects; but tasks don't do redirects.)
      return serviceClientFactory.makeClient(taskId, locator, retryPolicy);
    }

    private ServiceRetryPolicy makeRetryPolicy(final String taskId, final boolean retry)
    {
      final StandardRetryPolicy baseRetryPolicy;

      if (retry) {
        baseRetryPolicy = StandardRetryPolicy.builder()
                                             .maxAttempts(httpRetries + 1)
                                             .minWaitMillis(MIN_RETRY_WAIT_SECONDS * 1000)
                                             .maxWaitMillis(MAX_RETRY_WAIT_SECONDS * 1000)
                                             .retryNotAvailable(false)
                                             .build();
      } else {
        baseRetryPolicy = StandardRetryPolicy.noRetries();
      }

      return new SpecificTaskRetryPolicy(taskId, baseRetryPolicy);
    }
  }

  static class SeekableStreamTaskLocator implements ServiceLocator
  {
    private static final String BASE_PATH = "/druid/worker/v1/chat";

    private final TaskInfoProvider taskInfoProvider;
    private final String taskId;

    SeekableStreamTaskLocator(TaskInfoProvider taskInfoProvider, String taskId)
    {
      this.taskInfoProvider = taskInfoProvider;
      this.taskId = taskId;
    }

    @Override
    public ListenableFuture<ServiceLocations> locate()
    {
      final Optional<TaskStatus> status = taskInfoProvider.getTaskStatus(taskId);
      if (status.isPresent() && status.get().isRunnable()) {
        final TaskLocation location = taskInfoProvider.getTaskLocation(taskId);

        if (location.getHost() == null) {
          return Futures.immediateFuture(ServiceLocations.forLocations(Collections.emptySet()));
        } else {
          return Futures.immediateFuture(
              ServiceLocations.forLocation(
                  new ServiceLocation(
                      location.getHost(),
                      location.getPort(),
                      location.getTlsPort(),
                      StringUtils.format("%s/%s", BASE_PATH, StringUtils.urlEncode(taskId))
                  )
              )
          );
        }
      } else {
        return Futures.immediateFuture(ServiceLocations.closed());
      }
    }

    @Override
    public void close()
    {
      // Nothing to do. Instance holds no state.
    }
  }
}
