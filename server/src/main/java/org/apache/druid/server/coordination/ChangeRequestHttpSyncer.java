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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.Stopwatch;
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

  public static final long HTTP_TIMEOUT_EXTRA_MS = 5000;

  private static final long MAX_RETRY_BACKOFF = TimeUnit.MINUTES.toMillis(2);

  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final ScheduledExecutorService executor;
  private final URL baseServerURL;
  private final String baseRequestPath;
  private final TypeReference<ChangeRequestsSnapshot<T>> responseTypeReferences;
  private final long serverTimeoutMS;
  private final long serverHttpTimeout;

  private final Duration maxUnstableDuration;
  private final Duration maxDelayBetweenSyncRequests;
  private final Duration maxDurationToWaitForSync;

  private final Listener<T> listener;

  private final CountDownLatch initializationLatch = new CountDownLatch(1);

  /**
   * Lock to implement proper start-then-stop semantics. Used to ensure that:
   * <ul>
   * <li>No state update happens after {@link #stop()}.</li>
   * <li>No sync is scheduled after {@link #stop()}.</li>
   * <li>Any pending sync is skipped when {@link #stop()} has been called.</li>
   * <li>Duplicate syncs are not scheduled on the executor.</li>
   * </ul>
   */
  private final LifecycleLock startStopLock = new LifecycleLock();

  private final String logIdentity;
  private int consecutiveFailedAttemptCount = 0;

  private final Stopwatch sinceSyncerStart = Stopwatch.createUnstarted();
  private final Stopwatch sinceLastSyncRequest = Stopwatch.createUnstarted();
  private final Stopwatch sinceLastSyncSuccess = Stopwatch.createUnstarted();
  private final Stopwatch sinceUnstable = Stopwatch.createUnstarted();

  @Nullable
  private ChangeRequestHistory.Counter counter = null;

  public ChangeRequestHttpSyncer(
      ObjectMapper smileMapper,
      HttpClient httpClient,
      ScheduledExecutorService executor,
      URL baseServerURL,
      String baseRequestPath,
      TypeReference<ChangeRequestsSnapshot<T>> responseTypeReferences,
      long serverTimeoutMS,
      long serverUnstabilityTimeout,
      Listener<T> listener
  )
  {
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.executor = executor;
    this.baseServerURL = baseServerURL;
    this.baseRequestPath = baseRequestPath;
    this.responseTypeReferences = responseTypeReferences;
    this.serverTimeoutMS = serverTimeoutMS;
    this.serverHttpTimeout = serverTimeoutMS + HTTP_TIMEOUT_EXTRA_MS;
    this.listener = listener;
    this.logIdentity = StringUtils.format("%s_%d", baseServerURL, System.currentTimeMillis());

    this.maxDurationToWaitForSync = Duration.millis(3 * serverHttpTimeout);
    this.maxDelayBetweenSyncRequests = Duration.millis(3 * serverHttpTimeout + MAX_RETRY_BACKOFF);
    this.maxUnstableDuration = Duration.millis(serverUnstabilityTimeout);
  }

  public void start()
  {
    synchronized (startStopLock) {
      if (!startStopLock.canStart()) {
        throw new ISE("Could not start sync for server[%s].", logIdentity);
      }
      try {
        log.info("Starting sync for server[%s].", logIdentity);
        startStopLock.started();
      }
      finally {
        startStopLock.exitStart();
      }

      safeRestart(sinceSyncerStart);
      addNextSyncToWorkQueue();
    }
  }

  public void stop()
  {
    synchronized (startStopLock) {
      if (!startStopLock.canStop()) {
        throw new ISE("Could not stop sync for server[%s].", logIdentity);
      }
      try {
        log.info("Stopping sync for server[%s].", logIdentity);
      }
      finally {
        startStopLock.exitStop();
      }

      log.info("Stopped sync for server[%s].", logIdentity);
    }
  }

  /**
   * Waits for the first successful sync with this server up to {@link #maxDurationToWaitForSync}.
   */
  public boolean awaitInitialization() throws InterruptedException
  {
    return initializationLatch.await(maxDurationToWaitForSync.getMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * Whether this server has been synced successfully at least once.
   */
  public boolean isInitialized()
  {
    return initializationLatch.getCount() == 0;
  }

  /**
   * Returns debugging information for printing, must not be used for any other purpose.
   */
  public Map<String, Object> getDebugInfo()
  {
    return ImmutableMap.of(
        "millisSinceLastRequest", sinceLastSyncRequest.millisElapsed(),
        "millisSinceLastSuccess", sinceLastSyncSuccess.millisElapsed(),
        "consecutiveFailedAttemptCount", consecutiveFailedAttemptCount,
        "syncScheduled", startStopLock.isStarted()
    );
  }

  /**
   * Whether this syncer should be reset. This method returning true typically
   * indicates a problem with the sync scheduler.
   *
   * @return true if the delay since the last request to the server (or since
   * syncer start in case of no request to the server) has exceeded
   * {@link #maxDelayBetweenSyncRequests}.
   */
  public boolean needsReset()
  {
    if (sinceLastSyncRequest.isRunning()) {
      return sinceLastSyncRequest.hasElapsed(maxDelayBetweenSyncRequests);
    } else {
      return sinceSyncerStart.hasElapsed(maxDelayBetweenSyncRequests);
    }
  }

  public long getUnstableTimeMillis()
  {
    return consecutiveFailedAttemptCount <= 0 ? 0 : sinceUnstable.millisElapsed();
  }

  /**
   * @return true if there have been no sync failures recently and the last
   * successful sync was not more than {@link #maxDurationToWaitForSync} ago.
   */
  public boolean isSyncedSuccessfully()
  {
    return consecutiveFailedAttemptCount <= 0
           && sinceLastSyncSuccess.hasNotElapsed(maxDurationToWaitForSync);
  }

  private void sendSyncRequest()
  {
    if (!startStopLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
      log.info("Skipping sync for server[%s] as syncer has not started yet.", logIdentity);
      return;
    }

    safeRestart(sinceLastSyncRequest);

    try {
      final String req = getRequestString();

      BytesAccumulatingResponseHandler responseHandler = new BytesAccumulatingResponseHandler();

      log.debug("Sending sync request to server[%s]", logIdentity);

      ListenableFuture<InputStream> syncRequestFuture = httpClient.go(
          new Request(HttpMethod.GET, new URL(baseServerURL, req))
              .addHeader(HttpHeaders.Names.ACCEPT, SmileMediaTypes.APPLICATION_JACKSON_SMILE)
              .addHeader(HttpHeaders.Names.CONTENT_TYPE, SmileMediaTypes.APPLICATION_JACKSON_SMILE),
          responseHandler,
          Duration.millis(serverHttpTimeout)
      );

      log.debug("Sent sync request to [%s]", logIdentity);

      Futures.addCallback(
          syncRequestFuture,
          new FutureCallback<>()
          {
            @Override
            public void onSuccess(InputStream stream)
            {
              synchronized (startStopLock) {
                if (!startStopLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
                  log.info("Not handling response for server[%s] as syncer has not started yet.", logIdentity);
                  return;
                }

                try {
                  final int responseCode = responseHandler.getStatus();
                  if (responseCode == HttpServletResponse.SC_NO_CONTENT) {
                    log.debug("Received NO CONTENT from server[%s]", logIdentity);
                    safeRestart(sinceLastSyncSuccess);
                    return;
                  } else if (responseCode != HttpServletResponse.SC_OK) {
                    handleFailure(new ISE("Received sync response [%d]", responseCode));
                    return;
                  }

                  log.debug("Received sync response from server[%s]", logIdentity);
                  ChangeRequestsSnapshot<T> changes = smileMapper.readValue(stream, responseTypeReferences);
                  log.debug("Finished reading sync response from server[%s]", logIdentity);

                  if (changes.isResetCounter()) {
                    log.info("Server[%s] requested resetCounter for reason[%s].", logIdentity, changes.getResetCause());
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

                  if (consecutiveFailedAttemptCount > 0) {
                    consecutiveFailedAttemptCount = 0;
                    sinceUnstable.reset();
                    log.info("Server[%s] synced successfully.", logIdentity);
                  }

                  safeRestart(sinceLastSyncSuccess);
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
                  log.info("Not handling sync failure for server[%s] as syncer has not started yet.", logIdentity);
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
    catch (Throwable th) {
      try {
        markServerUnstableAndAlert(th, "Sending Request");
      }
      finally {
        addNextSyncToWorkQueue();
      }
    }
  }

  private String getRequestString()
  {
    final String req;
    if (counter != null) {
      req = StringUtils.format(
          "%s?counter=%s&hash=%s&timeout=%s",
          baseRequestPath,
          counter.getCounter(),
          counter.getHash(),
          serverTimeoutMS
      );
    } else {
      req = StringUtils.format("%s?counter=-1&timeout=%s", baseRequestPath, serverTimeoutMS);
    }
    return req;
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
          long delayMillis = Math.min(
              MAX_RETRY_BACKOFF,
              RetryUtils.nextRetrySleepMillis(consecutiveFailedAttemptCount)
          );
          log.info("Scheduling next sync for server[%s] in [%d] millis.", logIdentity, delayMillis);
          executor.schedule(this::sendSyncRequest, delayMillis, TimeUnit.MILLISECONDS);
        } else {
          executor.execute(this::sendSyncRequest);
        }
      }
      catch (Throwable th) {
        if (executor.isShutdown()) {
          log.warn(th, "Could not schedule sync for server[%s] because executor is stopped.", logIdentity);
        } else {
          log.warn(
              th,
              "Could not schedule sync for server [%s]. This syncer will be reset automatically."
              + " If the issue persists, try restarting this Druid service.",
              logIdentity
          );
        }
      }
    }
  }

  private void safeRestart(Stopwatch stopwatch)
  {
    synchronized (startStopLock) {
      stopwatch.restart();
    }
  }

  private void markServerUnstableAndAlert(Throwable throwable, String action)
  {
    if (consecutiveFailedAttemptCount++ == 0) {
      safeRestart(sinceUnstable);
    }

    final long unstableSeconds = getUnstableTimeMillis() / 1000;
    final String message = StringUtils.format(
        "Sync failed for server[%s] while [%s]. Failed [%d] times in the last [%d] seconds.",
        baseServerURL, action, consecutiveFailedAttemptCount, unstableSeconds
    );

    // Alert if unstable alert timeout has been exceeded
    if (sinceUnstable.hasElapsed(maxUnstableDuration)) {
      String alertMessage = StringUtils.format(
          "%s. Try restarting the Druid process on server[%s].",
          message, baseServerURL
      );
      log.noStackTrace().makeAlert(throwable, alertMessage).emit();
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
