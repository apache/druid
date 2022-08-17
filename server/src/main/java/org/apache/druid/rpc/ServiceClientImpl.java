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

package org.apache.druid.rpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.java.util.http.client.response.ObjectOrErrorResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Production implementation of {@link ServiceClient}.
 */
public class ServiceClientImpl implements ServiceClient
{
  private static final Logger log = new Logger(ServiceClientImpl.class);

  private final String serviceName;
  private final HttpClient httpClient;
  private final ServiceLocator serviceLocator;
  private final ServiceRetryPolicy retryPolicy;
  private final ScheduledExecutorService connectExec;

  // Populated when we receive a redirect. The location here has no base path; it only identifies a preferred server.
  private final AtomicReference<ServiceLocation> preferredLocationNoPath = new AtomicReference<>();

  public ServiceClientImpl(
      final String serviceName,
      final HttpClient httpClient,
      final ServiceLocator serviceLocator,
      final ServiceRetryPolicy retryPolicy,
      final ScheduledExecutorService connectExec
  )
  {
    this.serviceName = Preconditions.checkNotNull(serviceName, "serviceName");
    this.httpClient = Preconditions.checkNotNull(httpClient, "httpClient");
    this.serviceLocator = Preconditions.checkNotNull(serviceLocator, "serviceLocator");
    this.retryPolicy = Preconditions.checkNotNull(retryPolicy, "retryPolicy");
    this.connectExec = Preconditions.checkNotNull(connectExec, "connectExec");

    if (retryPolicy.maxAttempts() == 0) {
      throw new IAE("Invalid maxAttempts[%d] in retry policy", retryPolicy.maxAttempts());
    }
  }

  @Override
  public <IntermediateType, FinalType> ListenableFuture<FinalType> asyncRequest(
      final RequestBuilder requestBuilder,
      final HttpResponseHandler<IntermediateType, FinalType> handler
  )
  {
    final SettableFuture<FinalType> retVal = SettableFuture.create();
    tryRequest(requestBuilder, handler, retVal, 0, 0);
    return retVal;
  }

  @Override
  public ServiceClientImpl withRetryPolicy(ServiceRetryPolicy newRetryPolicy)
  {
    return new ServiceClientImpl(serviceName, httpClient, serviceLocator, newRetryPolicy, connectExec);
  }

  private <IntermediateType, FinalType> void tryRequest(
      final RequestBuilder requestBuilder,
      final HttpResponseHandler<IntermediateType, FinalType> handler,
      final SettableFuture<FinalType> retVal,
      final long attemptNumber,
      final int redirectCount
  )
  {
    whenServiceReady(
        serviceLocation -> {
          if (retVal.isCancelled()) {
            // Return early if the caller canceled the return future.
            return;
          }

          final long nextAttemptNumber = attemptNumber + 1;

          if (serviceLocation == null) {
            // Null location means the service is not currently available. Trigger a retry.
            final long backoffMs = computeBackoffMs(retryPolicy, attemptNumber);

            if (shouldTry(nextAttemptNumber)) {
              log.info(
                  "Service [%s] not available on attempt #%d; retrying in %,d ms.",
                  serviceName,
                  nextAttemptNumber,
                  backoffMs
              );

              connectExec.schedule(
                  () -> tryRequest(requestBuilder, handler, retVal, attemptNumber + 1, redirectCount),
                  backoffMs,
                  TimeUnit.MILLISECONDS
              );
            } else {
              retVal.setException(new ServiceNotAvailableException(serviceName));
            }

            return;
          }

          final Request request = requestBuilder.build(serviceLocation);
          ListenableFuture<Either<StringFullResponseHolder, FinalType>> responseFuture;

          log.debug("Service [%s] request [%s %s] starting.", serviceName, request.getMethod(), request.getUrl());

          responseFuture = httpClient.go(
              request,
              new ObjectOrErrorResponseHandler<>(handler),
              requestBuilder.getTimeout()
          );

          // Add cancellation listener on the return future to ensure that responseFuture is canceled too.
          final ListenableFuture<Either<StringFullResponseHolder, FinalType>> theResponseFuture = responseFuture;

          retVal.addListener(
              () -> {
                if (retVal.isCancelled()) {
                  theResponseFuture.cancel(true);
                }
              },
              Execs.directExecutor()
          );

          Futures.addCallback(
              responseFuture,
              new FutureCallback<Either<StringFullResponseHolder, FinalType>>()
              {
                @Override
                public void onSuccess(@Nullable final Either<StringFullResponseHolder, FinalType> result)
                {
                  try {
                    // result can be null if the HttpClient encounters a problem midstream on an unfinished response.
                    if (result != null && result.isValue()) {
                      if (nextAttemptNumber > 1) {
                        // There were retries. Log at INFO level to provide the user some closure.
                        log.info(
                            "Service [%s] request [%s %s] completed.",
                            serviceName,
                            request.getMethod(),
                            request.getUrl()
                        );
                      } else {
                        // No retries. Log at debug level to avoid cluttering the logs.
                        log.debug(
                            "Service [%s] request [%s %s] completed.",
                            serviceName,
                            request.getMethod(),
                            request.getUrl()
                        );
                      }

                      // Will not throw, because we checked result.isValue() earlier.
                      retVal.set(result.valueOrThrow());
                    } else {
                      final StringFullResponseHolder errorHolder = result != null ? result.error() : null;

                      if (errorHolder != null && isRedirect(errorHolder.getResponse().getStatus())) {
                        // Redirect. Update preferredLocationNoPath if appropriate, then reissue.
                        final String newUri = result.error().getResponse().headers().get("Location");

                        if (redirectCount >= MAX_REDIRECTS) {
                          retVal.setException(new RpcException(
                              "Service [%s] redirected too many times [%d] to invalid url %s",
                              serviceName,
                              redirectCount,
                              newUri
                          ));
                        } else {
                          // Update preferredLocationNoPath if we got a redirect.
                          final ServiceLocation redirectLocationNoPath = serviceLocationNoPathFromUri(newUri);

                          if (redirectLocationNoPath != null) {
                            preferredLocationNoPath.set(redirectLocationNoPath);
                            connectExec.submit(
                                () -> tryRequest(requestBuilder, handler, retVal, attemptNumber, redirectCount + 1)
                            );
                          } else {
                            retVal.setException(
                                new RpcException(
                                    "Service [%s] redirected [%d] times to invalid URL [%s]",
                                    serviceName,
                                    redirectCount,
                                    newUri
                                )
                            );
                          }
                        }
                      } else if (shouldTry(nextAttemptNumber)
                                 && (errorHolder == null || retryPolicy.retryHttpResponse(errorHolder.getResponse()))) {
                        // Retryable server response (or null errorHolder, which means null result, which can happen
                        // if the HttpClient encounters an exception in the midst of response processing).
                        final long backoffMs = computeBackoffMs(retryPolicy, attemptNumber);
                        log.noStackTrace().info(buildErrorMessage(request, errorHolder, backoffMs, nextAttemptNumber));
                        connectExec.schedule(
                            () -> tryRequest(requestBuilder, handler, retVal, attemptNumber + 1, redirectCount),
                            backoffMs,
                            TimeUnit.MILLISECONDS
                        );
                      } else if (errorHolder != null) {
                        // Nonretryable server response.
                        retVal.setException(new HttpResponseException(errorHolder));
                      } else {
                        // Nonretryable null result from the HTTP client.
                        retVal.setException(new RpcException(buildErrorMessage(request, null, -1, nextAttemptNumber)));
                      }
                    }
                  }
                  catch (Throwable t) {
                    // It's a bug if this happens. The purpose of this line is to help us debug what went wrong.
                    retVal.setException(new RpcException(t, "Service [%s] handler exited unexpectedly", serviceName));
                  }
                }

                @Override
                public void onFailure(final Throwable t)
                {
                  try {
                    final long nextAttemptNumber = attemptNumber + 1;

                    if (shouldTry(nextAttemptNumber) && retryPolicy.retryThrowable(t)) {
                      final long backoffMs = computeBackoffMs(retryPolicy, attemptNumber);

                      log.noStackTrace().info(t, buildErrorMessage(request, null, backoffMs, nextAttemptNumber));

                      connectExec.schedule(
                          () -> tryRequest(requestBuilder, handler, retVal, attemptNumber + 1, redirectCount),
                          backoffMs,
                          TimeUnit.MILLISECONDS
                      );
                    } else {
                      retVal.setException(new RpcException(t, buildErrorMessage(request, null, -1, nextAttemptNumber)));
                    }
                  }
                  catch (Throwable t2) {
                    // It's a bug if this happens. The purpose of this line is to help us debug what went wrong.
                    retVal.setException(new RpcException(t, "Service [%s] handler exited unexpectedly", serviceName));
                  }
                }
              },
              connectExec
          );
        },
        retVal
    );
  }

  private <T> void whenServiceReady(final Consumer<ServiceLocation> callback, final SettableFuture<T> retVal)
  {
    Futures.addCallback(
        serviceLocator.locate(),
        new FutureCallback<ServiceLocations>()
        {
          @Override
          public void onSuccess(final ServiceLocations locations)
          {
            if (locations.isClosed()) {
              retVal.setException(new ServiceClosedException(serviceName));
              return;
            }

            try {
              final ServiceLocation location = pick(locations);
              callback.accept(location);
            }
            catch (Throwable t) {
              // It's a bug if this happens. The purpose of this line is to help us debug what went wrong.
              retVal.setException(new RpcException(t, "Service [%s] handler exited unexpectedly", serviceName));
            }
          }

          @Override
          public void onFailure(Throwable t)
          {
            // Service locator exceptions are not recoverable.
            retVal.setException(new RpcException(t, "Service [%s] locator encountered exception", serviceName));
          }
        },
        connectExec
    );
  }

  @Nullable
  private ServiceLocation pick(final ServiceLocations locations)
  {
    final ServiceLocation preferred = preferredLocationNoPath.get();

    if (preferred != null) {
      // Preferred location is set. Use it if it's one of known locations.
      for (final ServiceLocation location : locations.getLocations()) {
        final ServiceLocation locationNoPath =
            new ServiceLocation(location.getHost(), location.getPlaintextPort(), location.getTlsPort(), "");

        if (locationNoPath.equals(preferred)) {
          return location;
        }
      }
    }

    // No preferred location, or, preferred location is not one of the known service locations. Go with the first one.
    return Iterables.getFirst(locations.getLocations(), null);
  }

  private boolean shouldTry(final long nextAttemptNumber)
  {
    return retryPolicy.maxAttempts() < 0 || nextAttemptNumber < retryPolicy.maxAttempts();
  }

  private String buildErrorMessage(
      final Request request,
      @Nullable final StringFullResponseHolder errorHolder,
      final long backoffMs,
      final long numAttempts
  )
  {
    final StringBuilder errorMessage = new StringBuilder();

    errorMessage.append("Service [")
                .append(serviceName)
                .append("] request [")
                .append(request.getMethod())
                .append(" ")
                .append(request.getUrl())
                .append("]");

    if (errorHolder != null) {
      final HttpResponseStatus httpResponseStatus = errorHolder.getStatus();
      errorMessage.append(" encountered server error [").append(httpResponseStatus).append("]");
    } else {
      errorMessage.append(" encountered exception");
    }

    errorMessage.append(" on attempt #").append(numAttempts);

    if (backoffMs > 0) {
      errorMessage.append("; retrying in ").append(StringUtils.format("%,d", backoffMs)).append(" ms");
    }

    if (errorHolder != null) {
      errorMessage.append("; ").append(HttpResponseException.choppedBodyErrorMessage(errorHolder.getContent()));
    }

    return errorMessage.toString();
  }

  @VisibleForTesting
  static long computeBackoffMs(final ServiceRetryPolicy retryPolicy, final long attemptNumber)
  {
    return Math.max(
        retryPolicy.minWaitMillis(),
        Math.min(retryPolicy.maxWaitMillis(), (long) (Math.pow(2, attemptNumber) * retryPolicy.minWaitMillis()))
    );
  }

  @Nullable
  @VisibleForTesting
  static ServiceLocation serviceLocationNoPathFromUri(@Nullable final String uriString)
  {
    if (uriString == null) {
      return null;
    }

    try {
      final URI uri = new URI(uriString);
      final String host = uri.getHost();

      if (host == null) {
        return null;
      }

      final String scheme = uri.getScheme();

      if ("http".equals(scheme)) {
        return new ServiceLocation(host, uri.getPort() < 0 ? 80 : uri.getPort(), -1, "");
      } else if ("https".equals(scheme)) {
        return new ServiceLocation(host, -1, uri.getPort() < 0 ? 443 : uri.getPort(), "");
      } else {
        return null;
      }
    }
    catch (URISyntaxException e) {
      return null;
    }
  }

  @VisibleForTesting
  static boolean isRedirect(final HttpResponseStatus responseStatus)
  {
    final int code = responseStatus.getCode();
    return code == HttpResponseStatus.TEMPORARY_REDIRECT.getCode()
           || code == HttpResponseStatus.FOUND.getCode()
           || code == HttpResponseStatus.MOVED_PERMANENTLY.getCode();
  }
}
