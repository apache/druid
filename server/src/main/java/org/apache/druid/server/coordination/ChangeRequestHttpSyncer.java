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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
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

  public static final long HTTP_TIMEOUT_EXTRA_MS = 5000;

  private static final long MAX_RETRY_BACKOFF = TimeUnit.MINUTES.toMillis(2);

  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final ScheduledExecutorService executor;
  private final URL baseServerURL;
  private final String baseRequestPath;
  private final TypeReference<ChangeRequestsSnapshot<T>> responseTypeReferences;
  private final long serverTimeoutMS;
  private final long serverUnstabilityTimeout;
  private final long serverHttpTimeout;

  private final Listener<T> listener;

  private final CountDownLatch initializationLatch = new CountDownLatch(1);

  /**
   * This lock is used to ensure proper start-then-stop semantics and making sure after stopping no state update happens
   * and {@link #sync} is not again scheduled in {@link #executor} and if there was a previously scheduled sync before
   * stopping, it is skipped and also, it is used to ensure that duplicate syncs are never scheduled in the executor.
   */
  private final LifecycleLock startStopLock = new LifecycleLock();

  private final String logIdentity;
  private long unstableStartTime = -1;
  private int consecutiveFailedAttemptCount = 0;
  private long lastSuccessfulSyncTime = 0;
  private long lastSyncTime = 0;

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
    this.serverUnstabilityTimeout = serverUnstabilityTimeout;
    this.serverHttpTimeout = serverTimeoutMS + HTTP_TIMEOUT_EXTRA_MS;
    this.listener = listener;
    this.logIdentity = StringUtils.format("%s_%d", baseServerURL, System.currentTimeMillis());
  }

  public void start()
  {
    synchronized (startStopLock) {
      if (!startStopLock.canStart()) {
        throw new ISE("Can't start ChangeRequestHttpSyncer[%s].", logIdentity);
      }

      log.info("Starting ChangeRequestHttpSyncer[%s].", logIdentity);
      startStopLock.started();
      startStopLock.exitStart();

      addNextSyncToWorkQueue();
    }
  }

  public void stop()
  {
    synchronized (startStopLock) {
      if (!startStopLock.canStop()) {
        throw new ISE("Can't stop ChangeRequestHttpSyncer[%s].", logIdentity);
      }

      log.info("Stopped ChangeRequestHttpSyncer[%s].", logIdentity);
    }
  }

  /** Wait for first fetch of segment listing from server. */
  public boolean awaitInitialization(long timeout, TimeUnit timeUnit) throws InterruptedException
  {
    return initializationLatch.await(timeout, timeUnit);
  }

  /**
   * This method returns the debugging information for printing, must not be used for any other purpose.
   */
  public Map<String, Object> getDebugInfo()
  {
    long currTime = System.currentTimeMillis();

    Object notSuccessfullySyncedFor;
    if (lastSuccessfulSyncTime == 0) {
      notSuccessfullySyncedFor = "Never Successfully Synced";
    } else {
      notSuccessfullySyncedFor = (currTime - lastSuccessfulSyncTime) / 1000;
    }
    return ImmutableMap.of(
        "notSyncedForSecs", lastSyncTime == 0 ? "Never Synced" : (currTime - lastSyncTime) / 1000,
        "notSuccessfullySyncedFor", notSuccessfullySyncedFor,
        "consecutiveFailedAttemptCount", consecutiveFailedAttemptCount,
        "syncScheduled", startStopLock.isStarted()
    );
  }

  /**
   * Exposed for monitoring use to see if sync is working fine and not stopped due to any coding bugs. If this
   * ever returns false then caller of this method must create an alert and it should be looked into for any
   * bugs.
   */
  public boolean isOK()
  {
    return (System.currentTimeMillis() - lastSyncTime) < MAX_RETRY_BACKOFF + 3 * serverHttpTimeout;
  }

  public long getServerHttpTimeout()
  {
    return serverHttpTimeout;
  }

  private void sync()
  {
    if (!startStopLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
      log.info("Skipping sync() call for server[%s].", logIdentity);
      return;
    }

    lastSyncTime = System.currentTimeMillis();

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
          new FutureCallback<InputStream>()
          {
            @Override
            public void onSuccess(InputStream stream)
            {
              synchronized (startStopLock) {
                if (!startStopLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
                  log.info("Skipping sync() success for server[%s].", logIdentity);
                  return;
                }

                try {
                  if (responseHandler.getStatus() == HttpServletResponse.SC_NO_CONTENT) {
                    log.debug("Received NO CONTENT from server[%s]", logIdentity);
                    lastSuccessfulSyncTime = System.currentTimeMillis();
                    return;
                  } else if (responseHandler.getStatus() != HttpServletResponse.SC_OK) {
                    handleFailure(new RE("Bad Sync Response."));
                    return;
                  }

                  log.debug("Received sync response from [%s]", logIdentity);

                  ChangeRequestsSnapshot<T> changes = smileMapper.readValue(stream, responseTypeReferences);

                  log.debug("Finished reading sync response from [%s]", logIdentity);

                  if (changes.isResetCounter()) {
                    log.info("[%s] requested resetCounter for reason [%s].", logIdentity, changes.getResetCause());
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
                    log.info("[%s] synced successfully for the first time.", logIdentity);
                  }

                  if (consecutiveFailedAttemptCount > 0) {
                    consecutiveFailedAttemptCount = 0;
                    log.info("[%s] synced successfully.", logIdentity);
                  }

                  lastSuccessfulSyncTime = System.currentTimeMillis();
                }
                catch (Exception ex) {
                  String logMsg = StringUtils.nonStrictFormat(
                      "Error processing sync response from [%s]. Reason [%s]",
                      logIdentity,
                      ex.getMessage()
                  );

                  if (incrementFailedAttemptAndCheckUnstabilityTimeout()) {
                    log.error(ex, logMsg);
                  } else {
                    log.info("Temporary Failure. %s", logMsg);
                    log.debug(ex, logMsg);
                  }
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
                  log.info("Skipping sync() failure for URL[%s].", logIdentity);
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
              String logMsg = StringUtils.nonStrictFormat(
                  "failed to get sync response from [%s]. Return code [%s], Reason: [%s]",
                  logIdentity,
                  responseHandler.getStatus(),
                  responseHandler.getDescription()
              );

              if (incrementFailedAttemptAndCheckUnstabilityTimeout()) {
                log.error(t, logMsg);
              } else {
                log.info("Temporary Failure. %s", logMsg);
                log.debug(t, logMsg);
              }
            }
          },
          executor
      );
    }
    catch (Throwable th) {
      try {
        String logMsg = StringUtils.nonStrictFormat(
            "Fatal error while fetching segment list from [%s].", logIdentity
        );

        if (incrementFailedAttemptAndCheckUnstabilityTimeout()) {
          log.makeAlert(th, logMsg).emit();
        } else {
          log.info("Temporary Failure. %s", logMsg);
          log.debug(th, logMsg);
        }
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
          long sleepMillis = Math.min(
              MAX_RETRY_BACKOFF,
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
          log.warn(
              th,
              "Couldn't schedule next sync. [%s] is not being synced any more, probably because executor is stopped.",
              logIdentity
          );
        } else {
          log.makeAlert(
              th,
              "WTF! Couldn't schedule next sync. [%s] is not being synced any more, restarting Druid process on that "
              + "server might fix the issue.",
              logIdentity
          ).emit();
        }
      }
    }
  }

  private boolean incrementFailedAttemptAndCheckUnstabilityTimeout()
  {
    if (consecutiveFailedAttemptCount > 0
        && (System.currentTimeMillis() - unstableStartTime) > serverUnstabilityTimeout) {
      return true;
    }

    if (consecutiveFailedAttemptCount++ == 0) {
      unstableStartTime = System.currentTimeMillis();
    }

    return false;
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
