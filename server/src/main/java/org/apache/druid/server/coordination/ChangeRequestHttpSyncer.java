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
import com.google.common.base.Stopwatch;
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

  public static final long MIN_READ_TIMEOUT_MILLIS = 5000;
  private static final long MAX_RETRY_BACKOFF_MILLIS = TimeUnit.MINUTES.toMillis(2);

  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final ScheduledExecutorService executor;
  private final URL baseServerURL;
  private final String baseRequestPath;
  private final TypeReference<ChangeRequestsSnapshot<T>> responseTypeReferences;

  private final long requestTimeoutMillis;
  private final Duration requestReadTimeout;

  private final long maxMillisToWaitForSync;

  /**
   * Max duration for which sync can be unstable before an alert is raised.
   */
  private final Duration maxUnstableDuration;

  /**
   * <pre>3 * {@link #requestReadTimeout} + {@link #MAX_RETRY_BACKOFF_MILLIS}</pre>
   */
  private final Duration maxDelayBetweenSyncRequests;

  private final Listener<T> listener;

  private final CountDownLatch initializationLatch = new CountDownLatch(1);

  /**
   * This lock is used to ensure proper start-then-stop semantics and making sure after stopping no state update happens
   * and {@link #sync} is not again scheduled in {@link #executor} and if there was a previously scheduled sync before
   * stopping, it is skipped and also, it is used to ensure that duplicate syncs are never scheduled in the executor.
   */
  private final LifecycleLock startStopLock = new LifecycleLock();

  private final String logIdentity;

  private int numRecentFailures;

  private final Stopwatch sinceSyncerStart = Stopwatch.createUnstarted();
  private final Stopwatch sinceUnstable = Stopwatch.createUnstarted();
  private final Stopwatch sinceLastSyncSuccess = Stopwatch.createUnstarted();
  private final Stopwatch sinceLastSyncRequest = Stopwatch.createUnstarted();

  @Nullable
  private ChangeRequestHistory.Counter counter = null;

  public ChangeRequestHttpSyncer(
      ObjectMapper smileMapper,
      HttpClient httpClient,
      ScheduledExecutorService executor,
      URL baseServerURL,
      String baseRequestPath,
      TypeReference<ChangeRequestsSnapshot<T>> responseTypeReferences,
      long requestTimeoutMillis,
      long maxUnstableMillis,
      Listener<T> listener
  )
  {
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.executor = executor;
    this.baseServerURL = baseServerURL;
    this.baseRequestPath = baseRequestPath;
    this.responseTypeReferences = responseTypeReferences;
    this.listener = listener;
    this.logIdentity = StringUtils.format("%s_%d", baseServerURL, System.currentTimeMillis());

    this.requestTimeoutMillis = requestTimeoutMillis;
    this.maxUnstableDuration = Duration.millis(maxUnstableMillis);

    final long readTimeoutMillis = requestTimeoutMillis + MIN_READ_TIMEOUT_MILLIS;
    this.requestReadTimeout = Duration.millis(readTimeoutMillis);
    this.maxMillisToWaitForSync = 3 * readTimeoutMillis;
    this.maxDelayBetweenSyncRequests = Duration.millis(3 * readTimeoutMillis + MAX_RETRY_BACKOFF_MILLIS);
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

      sinceSyncerStart.reset().start();
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
   * Waits for the first successful sync with this server up to .
   */
  public boolean awaitInitialization() throws InterruptedException
  {
    return initializationLatch.await(maxMillisToWaitForSync, TimeUnit.MILLISECONDS);
  }

  /**
   * Waits upto 1 milliseconds for the first successful sync with this server.
   */
  public boolean isInitialized() throws InterruptedException
  {
    return initializationLatch.await(1, TimeUnit.MILLISECONDS);
  }

  /**
   * Returns debugging information for printing, must not be used for any other purpose.
   */
  public Map<String, Object> getDebugInfo()
  {
    return ImmutableMap.of(
        "millisSinceLastSync",
        sinceLastSyncRequest.isRunning() ? DateTimes.millisElapsed(sinceLastSyncRequest) : "Never synced",
        "millisSinceLastSuccess",
        sinceLastSyncSuccess.isRunning() ? DateTimes.millisElapsed(sinceLastSyncSuccess) : "Never synced successfully",
        "millisUnstableDuration",
        sinceUnstable.isRunning() ? DateTimes.millisElapsed(sinceUnstable) : "Stable",
        "numRecentFailures", numRecentFailures,
        "syncScheduled", startStopLock.isStarted()
    );
  }

  private boolean hasSyncedSuccessfullyOnce()
  {
    return initializationLatch.getCount() <= 0;
  }

  /**
   * Whether this syncer should be reset. This method returning true typically
   * indicates a problem with the sync scheduler.
   *
   * @return true if the delay since the last sync request sent to the server
   * has exceeded {@link #maxDelayBetweenSyncRequests}, false otherwise.
   */
  public boolean needsReset()
  {
    return DateTimes.hasElapsed(maxDelayBetweenSyncRequests, sinceLastSyncRequest);
  }

  /**
   * @return true if there have been no sync failures recently and the last
   * successful sync was not more than {@link #maxMillisToWaitForSync} ago.
   */
  public boolean isSyncedSuccessfully()
  {
    final Duration timeoutDuration = Duration.millis(maxMillisToWaitForSync);
    if (numRecentFailures > 0) {
      return false;
    } else if (hasSyncedSuccessfullyOnce()) {
      return DateTimes.hasNotElapsed(timeoutDuration, sinceLastSyncSuccess);
    } else {
      return DateTimes.hasNotElapsed(timeoutDuration, sinceSyncerStart);
    }
  }

  public long getUnstableTimeMillis()
  {
    return numRecentFailures <= 0 ? 0 : DateTimes.millisElapsed(sinceUnstable);
  }

  private void sync()
  {
    if (!startStopLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
      log.info("Skipping sync for server[%s] as lifecycle has not started.", logIdentity);
      return;
    }

    sinceLastSyncRequest.reset().start();

    try {
      final String req = getRequestString();

      BytesAccumulatingResponseHandler responseHandler = new BytesAccumulatingResponseHandler();

      log.debug("Sending sync request to server[%s]", logIdentity);

      ListenableFuture<InputStream> syncRequestFuture = httpClient.go(
          new Request(HttpMethod.GET, new URL(baseServerURL, req))
              .addHeader(HttpHeaders.Names.ACCEPT, SmileMediaTypes.APPLICATION_JACKSON_SMILE)
              .addHeader(HttpHeaders.Names.CONTENT_TYPE, SmileMediaTypes.APPLICATION_JACKSON_SMILE),
          responseHandler,
          requestReadTimeout
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
                    sinceLastSyncSuccess.reset().start();
                    return;
                  } else if (responseStatus != HttpServletResponse.SC_OK) {
                    handleFailure(new ISE("Received sync response [%d]", responseStatus));
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

                  if (initializationLatch.getCount() > 0) {
                    initializationLatch.countDown();
                    log.info("Server[%s] synced successfully for the first time.", logIdentity);
                  }

                  if (numRecentFailures > 0) {
                    numRecentFailures = 0;
                    sinceUnstable.reset();
                    log.info("Server[%s] synced successfully.", logIdentity);
                  }

                  sinceLastSyncSuccess.reset().start();
                }
                catch (Exception ex) {
                  markServerUnstableAndAlert(ex, "Processing Response");
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
        markServerUnstableAndAlert(t, "Sending Request");
      }
      finally {
        addNextSyncToWorkQueue();
      }
    }
  }

  private String getRequestString()
  {
    if (counter == null) {
      return StringUtils.format(
          "%s?counter=-1&timeout=%s",
          baseRequestPath, requestTimeoutMillis
      );
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
        if (numRecentFailures > 0) {
          long sleepMillis = Math.min(
              MAX_RETRY_BACKOFF_MILLIS,
              RetryUtils.nextRetrySleepMillis(numRecentFailures)
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

  private void markServerUnstableAndAlert(Throwable throwable, String action)
  {
    if (numRecentFailures++ == 0) {
      sinceUnstable.reset().start();
    }

    final long unstableSeconds = getUnstableTimeMillis() / 1000;
    final String message = StringUtils.format(
        "Sync failed for server[%s] while [%s]. Failed [%d] times in the last [%d] seconds.",
        baseServerURL, action, numRecentFailures, unstableSeconds
    );

    // Alert if unstable alert timeout has been exceeded
    if (DateTimes.hasElapsed(maxUnstableDuration, sinceUnstable)) {
      log.noStackTrace().makeAlert(throwable, message).emit();
    } else if (log.isDebugEnabled()) {
      log.debug(throwable, message);
    } else {
      log.noStackTrace().info(throwable, message);
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
