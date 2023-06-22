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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.server.coordinator.BytesAccumulatingResponseHandler;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class facilitates the usage of long-polling HTTP endpoints powered by {@link ChangeRequestHistory}.
 * For example {@link org.apache.druid.client.HttpServerInventoryView} uses it to keep segment state in sync with data
 * nodes which expose the segment state via HTTP endpoint in {@link
 * org.apache.druid.server.http.SegmentListerResource#getSegments}.
 */
public class ChangeRequestHttpSyncer<T>
{
  private static final EmittingLogger log = new EmittingLogger(ChangeRequestHttpSyncer.class);

  public static final long HTTP_TIMEOUT_EXTRA_MILLIS = 5000;

  private static final long MAX_RETRY_BACKOFF_MILLIS = TimeUnit.MINUTES.toMillis(2);

  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final ScheduledExecutorService executor;
  private final URL baseServerURL;
  private final String baseRequestPath;
  private final TypeReference<ChangeRequestsSnapshot<T>> responseTypeReferences;
  private final long serverHttpTimeoutMillis;

  private final Duration requestTimeout;
  private final Duration unstableAlertTimeout;
  private final Duration syncTimeout;

  private final Listener<T> listener;

  private final CountDownLatch successfulSyncLatch = new CountDownLatch(1);

  /**
   * This lock is used to ensure proper start-then-stop semantics and making sure after stopping no state update happens
   * and {@link #sync} is not again scheduled in {@link #executor} and if there was a previously scheduled sync before
   * stopping, it is skipped and also, it is used to ensure that duplicate syncs are never scheduled in the executor.
   */
  private final LifecycleLock startStopLock = new LifecycleLock();

  private final String logIdentity;

  private long syncerStartTimeNanos;
  private long unstableStartTimeNanos;
  private int consecutiveFailedAttemptCount;
  private long lastSuccessfulSyncTimeNanos;
  private long lastSyncTimeNanos;

  @Nullable
  private ChangeRequestHistory.Counter counter = null;

  public ChangeRequestHttpSyncer(
      ObjectMapper smileMapper,
      HttpClient httpClient,
      ScheduledExecutorService executor,
      URL baseServerURL,
      String baseRequestPath,
      TypeReference<ChangeRequestsSnapshot<T>> responseTypeReferences,
      Duration requestTimeout,
      Duration unstableAlertTimeout,
      Listener<T> listener
  )
  {
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.executor = executor;
    this.baseServerURL = baseServerURL;
    this.baseRequestPath = baseRequestPath;
    this.responseTypeReferences = responseTypeReferences;
    this.serverHttpTimeoutMillis = requestTimeout.getMillis() + HTTP_TIMEOUT_EXTRA_MILLIS;
    this.listener = listener;
    this.logIdentity = StringUtils.format("%s_%d", baseServerURL, System.currentTimeMillis());

    this.requestTimeout = requestTimeout;
    this.unstableAlertTimeout = unstableAlertTimeout;
    this.syncTimeout = Duration.millis(MAX_RETRY_BACKOFF_MILLIS + 3 * serverHttpTimeoutMillis);
  }

  public void start()
  {
    synchronized (startStopLock) {
      if (!startStopLock.canStart()) {
        throw new ISE("Can't start ChangeRequestHttpSyncer[%s].", logIdentity);
      }

      try {
        log.info("Starting ChangeRequestHttpSyncer[%s].", logIdentity);
        startStopLock.started();
      }
      finally {
        startStopLock.exitStart();
      }

      syncerStartTimeNanos = System.nanoTime();
      addNextSyncToWorkQueue();
    }
  }

  public void stop()
  {
    synchronized (startStopLock) {
      if (!startStopLock.canStop()) {
        throw new ISE("Can't stop ChangeRequestHttpSyncer[%s].", logIdentity);
      }
      try {
        log.info("Stopping ChangeRequestHttpSyncer[%s].", logIdentity);
      }
      finally {
        startStopLock.exitStop();
      }

      log.info("Stopped ChangeRequestHttpSyncer[%s].", logIdentity);
    }
  }

  /**
   * Waits for the first successful sync with this server.
   */
  public boolean awaitInitialization(long timeout, TimeUnit timeUnit) throws InterruptedException
  {
    return successfulSyncLatch.await(timeout, timeUnit);
  }

  /**
   * Returns debugging information for printing, must not be used for any other purpose.
   */
  public Map<String, Object> getDebugInfo()
  {
    return ImmutableMap.of(
        "lastSyncDelayMillis",
        lastSyncTimeNanos == 0 ? "Never synced" : DateTimes.millisElapsedSince(lastSyncTimeNanos),
        "lastSuccessfulSyncDelayMillis",
        hasSyncedSuccessfullyOnce() ? DateTimes.millisElapsedSince(lastSuccessfulSyncTimeNanos)
                                    : "Never synced successfully",
        "unstableStartDelayMillis",
        unstableStartTimeNanos == 0 ? "Stable" : DateTimes.millisElapsedSince(unstableStartTimeNanos),
        "consecutiveFailedAttemptCount", consecutiveFailedAttemptCount,
        "syncScheduled", startStopLock.isStarted()
    );
  }

  private boolean hasSyncedSuccessfullyOnce()
  {
    return successfulSyncLatch.getCount() <= 0;
  }

  /**
   * Exposed for monitoring use to see if sync is working fine and not stopped due to any coding bugs. If this
   * ever returns false then caller of this method must create an alert and it should be looked into for any
   * bugs.
   */
  public boolean hasSyncedRecently()
  {
    return !DateTimes.hasElapsedSince(syncTimeout, lastSyncTimeNanos);
  }

  /**
   * @return true if there have been no sync failures recently and the last sync
   * was not more than {@code 3 * serverHttpTimeoutMillis} ago.
   */
  public boolean isSyncingSuccessfully()
  {
    final Duration timeoutDuration = Duration.millis(3 * serverHttpTimeoutMillis);
    if (consecutiveFailedAttemptCount > 0) {
      return false;
    } else if (hasSyncedSuccessfullyOnce()) {
      return !DateTimes.hasElapsedSince(timeoutDuration, lastSuccessfulSyncTimeNanos);
    } else {
      return !DateTimes.hasElapsedSince(timeoutDuration, syncerStartTimeNanos);
    }
  }

  public long getUnstableTimeMillis()
  {
    return consecutiveFailedAttemptCount <= 0
           ? 0 : DateTimes.millisElapsedSince(unstableStartTimeNanos);
  }

  public long getServerHttpTimeoutMillis()
  {
    return serverHttpTimeoutMillis;
  }

  private void sync()
  {
    if (!startStopLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
      log.info("Skipping sync for server[%s] as lifecycle has not started.", logIdentity);
      return;
    }

    lastSyncTimeNanos = System.nanoTime();

    try {
      final String req = getRequestString();

      BytesAccumulatingResponseHandler responseHandler = new BytesAccumulatingResponseHandler();

      log.debug("Sending sync request to server[%s]", logIdentity);

      ListenableFuture<InputStream> syncRequestFuture = httpClient.go(
          new Request(HttpMethod.GET, new URL(baseServerURL, req))
              .addHeader(HttpHeaders.Names.ACCEPT, SmileMediaTypes.APPLICATION_JACKSON_SMILE)
              .addHeader(HttpHeaders.Names.CONTENT_TYPE, SmileMediaTypes.APPLICATION_JACKSON_SMILE),
          responseHandler,
          Duration.millis(serverHttpTimeoutMillis)
      );

      log.debug("Sent sync request to server[%s]", logIdentity);

      Futures.addCallback(
          syncRequestFuture,
          new FutureCallback<InputStream>()
          {
            @Override
            public void onSuccess(InputStream stream)
            {
              synchronized (startStopLock) {
                if (!startStopLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
                  log.info("Not handling sync response for server[%s] as lifecycle has not started.", logIdentity);
                  return;
                }

                try {
                  final int responseStatus = responseHandler.getStatus();
                  if (responseStatus == HttpServletResponse.SC_NO_CONTENT) {
                    log.info("Received NO CONTENT from server[%s]", logIdentity);
                    lastSuccessfulSyncTimeNanos = System.nanoTime();
                    return;
                  } else if (responseStatus != HttpServletResponse.SC_OK) {
                    handleFailure(new ISE("Received invalid sync response"));
                    return;
                  }

                  log.debug("Received response from server[%s]", logIdentity);
                  ChangeRequestsSnapshot<T> changes = smileMapper.readValue(stream, responseTypeReferences);
                  log.debug("Finished reading response from server[%s]", logIdentity);

                  if (changes.isResetCounter()) {
                    log.info(
                        "Server[%s] requested resetCounter for reason [%s].",
                        logIdentity, changes.getResetCause()
                    );
                    counter = null;
                    return;
                  }

                  if (counter == null) {
                    listener.fullSync(changes.getRequests());
                  } else {
                    listener.deltaSync(changes.getRequests());
                  }

                  counter = changes.getCounter();

                  if (successfulSyncLatch.getCount() > 0) {
                    successfulSyncLatch.countDown();
                    log.info("Server[%s] synced successfully for the first time.", logIdentity);
                  }

                  if (consecutiveFailedAttemptCount > 0) {
                    consecutiveFailedAttemptCount = 0;
                    unstableStartTimeNanos = 0;
                    log.info("Server[%s] synced successfully.", logIdentity);
                  }

                  lastSuccessfulSyncTimeNanos = System.nanoTime();
                }
                catch (Exception ex) {
                  markServerUnstableAndAlert(ex, "processing sync response");
                }
                finally {
                  addNextSyncToWorkQueue();
                }
              }
            }

            @Override
            public void onFailure(Throwable t)
            {
              synchronized (startStopLock) {
                if (!startStopLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
                  log.info("Not handling sync failure for server[%s] as lifecycle has not started.", logIdentity);
                  return;
                }

                try {
                  handleFailure(t);
                }
                finally {
                  addNextSyncToWorkQueue();
                }
              }
            }

            private void handleFailure(Throwable t)
            {
              String logMsg = StringUtils.format(
                  "Handling response with code[%d], description[%s]",
                  responseHandler.getStatus(),
                  responseHandler.getDescription()
              );
              markServerUnstableAndAlert(t, logMsg);
            }
          },
          executor
      );
    }
    catch (Throwable t) {
      try {
        markServerUnstableAndAlert(t, "sending sync request");
      }
      finally {
        addNextSyncToWorkQueue();
      }
    }
  }

  private String getRequestString()
  {
    final long requestTimeoutMillis = requestTimeout.getMillis();
    if (counter == null) {
      return StringUtils.format("%s?counter=-1&timeout=%s", baseRequestPath, requestTimeoutMillis);
    } else {
      return StringUtils.format(
          "%s?counter=%s&hash=%s&timeout=%s",
          baseRequestPath, counter.getCounter(), counter.getHash(), requestTimeoutMillis
      );
    }
  }

  private void addNextSyncToWorkQueue()
  {
    synchronized (startStopLock) {
      if (!startStopLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
        log.info("Not scheduling sync for server[%s]. Instance stopped.", logIdentity);
        return;
      }

      try {
        if (consecutiveFailedAttemptCount > 0) {
          long sleepMillis = Math.min(
              MAX_RETRY_BACKOFF_MILLIS,
              RetryUtils.nextRetrySleepMillis(consecutiveFailedAttemptCount)
          );
          log.info("Scheduling next syncup in [%d] millis for server[%s].", sleepMillis, logIdentity);
          executor.schedule(this::sync, sleepMillis, TimeUnit.MILLISECONDS);
        } else {
          executor.execute(this::sync);
        }
      }
      catch (Throwable th) {
        if (executor.isShutdown()) {
          log.warn(th, "Could not schedule sync for server[%s] because executor is stopped.", logIdentity);
        } else {
          log.makeAlert(
              th,
              "Could not schedule sync for server[%s]. Try restarting the Druid process on that server.",
              logIdentity
          ).emit();
        }
      }
    }
  }

  private void markServerUnstableAndAlert(Throwable error, String action)
  {
    if (consecutiveFailedAttemptCount++ == 0) {
      unstableStartTimeNanos = System.nanoTime();
    }

    final long unstableSeconds = getUnstableTimeMillis() / 1000;
    final String message = StringUtils.format(
        "Sync failed for server[%s] while [%s]. Already failed [%d] times in the last [%d] seconds.",
        baseServerURL, action, consecutiveFailedAttemptCount, unstableSeconds
    );

    // Alert if unstable alert timeout has been exceeded
    if (DateTimes.hasElapsedSince(unstableAlertTimeout, unstableStartTimeNanos)) {
      log.makeAlert(error, "Server[%s] has been unstable for [%d] seconds", baseServerURL, unstableSeconds)
         .addData("message", message)
         .emit();
    } else if (log.isDebugEnabled()) {
      log.debug(error, message);
    } else {
      log.noStackTrace().info(error, message);
    }
  }

  @VisibleForTesting
  public boolean isExecutorShutdown()
  {
    return executor.isShutdown();
  }

  /**
   * Concurrency guarantees: all calls to {@link #fullSync} and {@link #deltaSync} (that is done within the {@link
   * #executor}) are linearizable.
   */
  public interface Listener<T>
  {
    /**
     * This method is called either if on the previous request the server had asked to reset the counter or it was the
     * first request to the server.
     */
    void fullSync(List<T> changes);

    void deltaSync(List<T> changes);
  }
}
