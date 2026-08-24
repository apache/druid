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

package org.apache.druid.server.coordinator.loading;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.config.Configs;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.server.coordination.DataSegmentChangeCallback;
import org.apache.druid.server.coordination.DataSegmentChangeHandler;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.DataSegmentChangeResponse;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.server.coordination.SegmentChangeStatus;
import org.apache.druid.server.coordinator.BytesAccumulatingResponseHandler;
import org.apache.druid.server.coordinator.config.HttpLoadQueuePeonConfig;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.CoordinatorStat;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.server.http.SegmentLoadingCapabilities;
import org.apache.druid.server.http.SegmentLoadingMode;
import org.apache.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 *
 */
public class HttpLoadQueuePeon implements LoadQueuePeon
{
  public static final TypeReference<List<DataSegmentChangeRequest>> REQUEST_ENTITY_TYPE_REF =
      new TypeReference<>() {};

  public static final TypeReference<List<DataSegmentChangeResponse>> RESPONSE_ENTITY_TYPE_REF =
      new TypeReference<>() {};

  private static final EmittingLogger log = new EmittingLogger(HttpLoadQueuePeon.class);
  private static final long DEFAULT_TIMEOUT = 10000L;

  private final AtomicLong queuedSize = new AtomicLong(0);
  private final AtomicReference<CoordinatorRunStats> stats = new AtomicReference<>(new CoordinatorRunStats());

  private final ConcurrentMap<DataSegment, SegmentHolder> segmentsToLoad = new ConcurrentHashMap<>();
  private final ConcurrentMap<DataSegment, SegmentHolder> segmentsToDrop = new ConcurrentHashMap<>();
  private final Set<DataSegment> segmentsMarkedToDrop = ConcurrentHashMap.newKeySet();
  private final LoadingRateTracker loadingRateTracker = new LoadingRateTracker();

  /**
   * Segments currently in queue ordered by priority and interval. This includes
   * drop requests as well. This need not be thread-safe as all operations on it
   * are synchronized with the {@link #lock}.
   */
  private final Set<SegmentHolder> queuedSegments = new TreeSet<>();

  /**
   * Set of segments for which requests have been sent to the server and can
   * not be cancelled anymore. This need not be thread-safe.
   */
  private final Set<DataSegment> activeRequestSegments = new HashSet<>();

  private final ScheduledExecutorService processingExecutor;

  private volatile boolean stopped = false;

  private final Object lock = new Object();

  private final HttpLoadQueuePeonConfig config;

  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final String serverId;

  private final AtomicBoolean mainLoopInProgress = new AtomicBoolean(false);
  private final ExecutorService callBackExecutor;
  private final Supplier<SegmentLoadingMode> loadingModeSupplier;

  private final ObjectWriter requestBodyWriter;

  /**
   * Loading capabilities of the server. Fetched during construction and re-fetched
   * lazily on subsequent ticks if the initial fetch fell back to default values due
   * to a transient failure (see {@link #refetchCapabilitiesIfNeeded()}). Read and
   * written only on the single-threaded processing executor after construction, but
   * declared {@code volatile} for safe publication from the constructing thread.
   */
  private volatile SegmentLoadingCapabilities serverCapabilities;

  /**
   * Whether {@link #serverCapabilities} holds a definitive value: {@code true} once
   * the server has returned real capabilities, or a 404 indicating the endpoint does
   * not exist on this server. It stays {@code false} while the peon is pinned to
   * default capabilities due to a transient failure, so the value is re-fetched once
   * the server recovers.
   */
  private volatile boolean capabilitiesConfirmed = false;

  /**
   * Guards {@link #refetchCapabilitiesIfNeeded()} so that at most one capability probe is
   * outstanding at a time. Without this, every {@link #doSegmentManagement()} tick issues its
   * own probe whenever {@link #capabilitiesConfirmed} is false, so queuing many segments onto a
   * still-unhealthy server fires one redundant concurrent probe per tick at that same server.
   */
  private final AtomicBoolean refetchInProgress = new AtomicBoolean(false);

  public HttpLoadQueuePeon(
      String baseUrl,
      ObjectMapper jsonMapper,
      HttpClient httpClient,
      HttpLoadQueuePeonConfig config,
      Supplier<SegmentLoadingMode> loadingModeSupplier,
      ScheduledExecutorService processingExecutor,
      ExecutorService callBackExecutor
  )
  {
    this.jsonMapper = jsonMapper;
    this.requestBodyWriter = jsonMapper.writerFor(REQUEST_ENTITY_TYPE_REF);
    this.httpClient = httpClient;
    this.config = config;
    this.processingExecutor = processingExecutor;
    this.callBackExecutor = callBackExecutor;

    this.serverId = baseUrl;
    this.loadingModeSupplier = loadingModeSupplier;
    this.serverCapabilities = fetchSegmentLoadingCapabilities();
  }

  private URL getLoadCapabilitiesUrl() throws MalformedURLException
  {
    return new URL(new URL(serverId), "druid-internal/v1/segments/loadCapabilities");
  }

  /**
   * Synchronously fetches loading capabilities during construction. On a transient failure
   * (non-OK status other than 404, timeout, or error), raises an alert and falls back to
   * default capabilities, leaving {@link #capabilitiesConfirmed} unset so the value is
   * re-fetched on a later tick once the server recovers (see {@link #refetchCapabilitiesIfNeeded()}).
   */
  private SegmentLoadingCapabilities fetchSegmentLoadingCapabilities()
  {
    try {
      final URL url = getLoadCapabilitiesUrl();
      final BytesAccumulatingResponseHandler responseHandler = new BytesAccumulatingResponseHandler();
      final InputStream stream = httpClient.go(
          new Request(HttpMethod.GET, url).addHeader(HttpHeaders.Names.ACCEPT, MediaType.APPLICATION_JSON),
          responseHandler,
          new Duration(DEFAULT_TIMEOUT)
      ).get();

      final int status = responseHandler.getStatus();
      final SegmentLoadingCapabilities capabilities = interpretCapabilitiesResponse(status, stream, url);
      if (!capabilitiesConfirmed) {
        // Transient failure. Do not stall further processing due to a single unhealthy server:
        // raise an alert and use default capabilities until the server recovers.
        log.makeAlert(
            "Received status[%s] when fetching loading capabilities from server[%s]. Using default values[%s].",
            status,
            serverId,
            capabilities
        ).emit();
      }
      return capabilities;
    }
    catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    }
    catch (Exception e) {
      SegmentLoadingCapabilities defaultCapabilities = getDefaultLoadingCapabilities();
      log.makeAlert(
          e,
          "Received error while fetching historical capabilities from Server[%s]. Using default values[%s].",
          serverId,
          defaultCapabilities
      ).emit();
      return defaultCapabilities;
    }
  }

  /**
   * Interprets a loadCapabilities response, setting {@link #capabilitiesConfirmed} and returning
   * the capabilities to use. The value is confirmed on a real response (200) or a 404 (the endpoint
   * is absent on this server, so retrying is pointless). A transient non-OK status yields default
   * capabilities without confirming, so they are re-fetched on a later tick once the server recovers.
   */
  private SegmentLoadingCapabilities interpretCapabilitiesResponse(int status, InputStream stream, URL url)
      throws IOException
  {
    if (HttpServletResponse.SC_NOT_FOUND == status) {
      capabilitiesConfirmed = true;
      SegmentLoadingCapabilities defaultCapabilities = getDefaultLoadingCapabilities();
      log.warn(
          "Historical capabilities endpoint not found at URL[%s]. Using default values[%s].",
          url,
          defaultCapabilities
      );
      return defaultCapabilities;
    } else if (HttpServletResponse.SC_OK != status) {
      return getDefaultLoadingCapabilities();
    }

    SegmentLoadingCapabilities capabilities = jsonMapper.readValue(stream, SegmentLoadingCapabilities.class);
    capabilitiesConfirmed = true;
    return capabilities;
  }

  /**
   * Re-fetches loading capabilities if the peon is still pinned to default values from a
   * transient failure during construction. Called on every segment management tick; a no-op
   * once capabilities have been confirmed (the common case).
   * <p>
   * Unlike the construction path, this does not block the (single, shared) processing thread:
   * the request is issued and its response handled in a callback, just like the segment change
   * requests in {@link #doSegmentManagement()}. This keeps a single unhealthy server from
   * stalling segment management for the rest of the cluster.
   */
  private void refetchCapabilitiesIfNeeded()
  {
    if (capabilitiesConfirmed || stopped || !refetchInProgress.compareAndSet(false, true)) {
      return;
    }

    try {
      final URL url = getLoadCapabilitiesUrl();
      final BytesAccumulatingResponseHandler responseHandler = new BytesAccumulatingResponseHandler();
      final ListenableFuture<InputStream> future = httpClient.go(
          new Request(HttpMethod.GET, url).addHeader(HttpHeaders.Names.ACCEPT, MediaType.APPLICATION_JSON),
          responseHandler,
          new Duration(DEFAULT_TIMEOUT)
      );

      Futures.addCallback(
          future,
          new FutureCallback<>()
          {
            @Override
            public void onSuccess(InputStream result)
            {
              try {
                serverCapabilities = interpretCapabilitiesResponse(responseHandler.getStatus(), result, url);
                if (capabilitiesConfirmed) {
                  log.info("Refreshed loading capabilities[%s] for server[%s].", serverCapabilities, serverId);
                }
              }
              catch (Throwable t) {
                log.debug(t, "Could not parse refreshed loading capabilities from server[%s]. Will retry.", serverId);
              }
              finally {
                refetchInProgress.set(false);
              }
            }

            @Override
            public void onFailure(Throwable t)
            {
              log.debug(t, "Could not refresh loading capabilities from server[%s]. Will retry.", serverId);
              refetchInProgress.set(false);
            }
          },
          processingExecutor
      );
    }
    catch (Throwable th) {
      log.debug(th, "Error issuing capability refresh request to server[%s]. Will retry.", serverId);
      refetchInProgress.set(false);
    }
  }

  private SegmentLoadingCapabilities getDefaultLoadingCapabilities()
  {
    int batchSize = config.getBatchSize() == null ? 1 : config.getBatchSize();
    return new SegmentLoadingCapabilities(batchSize, batchSize);
  }

  private void doSegmentManagement()
  {
    // Re-fetch loading capabilities if we are still pinned to defaults from a transient
    // failure. This is async and a no-op once capabilities are confirmed, so it runs
    // independently of the main loop below (which may bail out early if already in progress).
    refetchCapabilitiesIfNeeded();

    if (stopped || !mainLoopInProgress.compareAndSet(false, true)) {
      log.trace("[%s]Ignoring tick. Either in-progress already or stopped.", serverId);
      return;
    }

    final SegmentLoadingMode loadingMode = loadingModeSupplier.get();
    final int batchSize = calculateBatchSize(loadingMode);

    final List<DataSegmentChangeRequest> newRequests = new ArrayList<>(batchSize);

    synchronized (lock) {
      final Iterator<SegmentHolder> queuedSegmentIterator = queuedSegments.iterator();

      while (newRequests.size() < batchSize && queuedSegmentIterator.hasNext()) {
        final SegmentHolder holder = queuedSegmentIterator.next();
        final DataSegment segment = holder.getSegment();
        if (holder.hasRequestTimedOut()) {
          onRequestFailed(holder, SegmentChangeStatus.failed("timed out"));
          queuedSegmentIterator.remove();
          if (holder.isLoad()) {
            segmentsToLoad.remove(segment);
          } else {
            segmentsToDrop.remove(segment);
          }
          activeRequestSegments.remove(segment);
        } else {
          newRequests.add(holder.getChangeRequest());
          holder.markRequestSentToServer();
          activeRequestSegments.add(segment);
        }
      }

      if (segmentsToLoad.isEmpty()) {
        loadingRateTracker.markBatchLoadingFinished();
      }
    }

    if (newRequests.isEmpty()) {
      log.trace(
          "[%s]Found no load/drop requests. SegmentsToLoad[%d], SegmentsToDrop[%d], batchSize[%d].",
          serverId, segmentsToLoad.size(), segmentsToDrop.size(), batchSize
      );
      mainLoopInProgress.set(false);
      return;
    }

    try {
      log.trace("Sending [%d] load/drop requests to Server[%s] in loadingMode[%s].", newRequests.size(), serverId, loadingMode);
      final boolean hasLoadRequests = newRequests.stream().anyMatch(r -> r instanceof SegmentChangeRequestLoad);
      if (hasLoadRequests && !loadingRateTracker.isLoadingBatch()) {
        loadingRateTracker.markBatchLoadingStarted();
      }

      final URL changeRequestURL = new URL(
          new URL(serverId),
          StringUtils.nonStrictFormat(
              "druid-internal/v1/segments/changeRequests?timeout=%d&loadingMode=%s",
              config.getHostTimeout().getMillis(),
              loadingMode
          )
      );

      BytesAccumulatingResponseHandler responseHandler = new BytesAccumulatingResponseHandler();
      ListenableFuture<InputStream> future = httpClient.go(
          new Request(HttpMethod.POST, changeRequestURL)
              .addHeader(HttpHeaders.Names.ACCEPT, MediaType.APPLICATION_JSON)
              .addHeader(HttpHeaders.Names.CONTENT_TYPE, MediaType.APPLICATION_JSON)
              .setContent(requestBodyWriter.writeValueAsBytes(newRequests)),
          responseHandler,
          new Duration(config.getHostTimeout().getMillis() + 5000)
      );

      Futures.addCallback(
          future,
          new FutureCallback<>()
          {
            @Override
            public void onSuccess(InputStream result)
            {
              boolean scheduleNextRunImmediately = true;
              try {
                if (responseHandler.getStatus() == HttpServletResponse.SC_NO_CONTENT) {
                  log.trace("Received NO CONTENT reseponse from [%s]", serverId);
                } else if (HttpServletResponse.SC_OK == responseHandler.getStatus()) {
                  try {
                    List<DataSegmentChangeResponse> statuses =
                        jsonMapper.readValue(result, RESPONSE_ENTITY_TYPE_REF);
                    log.trace("Server[%s] returned status response [%s].", serverId, statuses);
                    synchronized (lock) {
                      if (stopped) {
                        log.trace("Ignoring response from Server[%s]. We are already stopped.", serverId);
                        scheduleNextRunImmediately = false;
                        return;
                      }

                      int numSuccessfulLoads = 0;
                      long successfulLoadSize = 0;
                      for (DataSegmentChangeResponse e : statuses) {
                        switch (e.getStatus().getState()) {
                          case SUCCESS:
                            if (e.getRequest() instanceof SegmentChangeRequestLoad) {
                              ++numSuccessfulLoads;
                              successfulLoadSize +=
                                  ((SegmentChangeRequestLoad) e.getRequest()).getSegment().getSize();
                            }
                          case FAILED:
                            handleResponseStatus(e.getRequest(), e.getStatus());
                            break;
                          case PENDING:
                            log.trace("Request[%s] is still pending on server[%s].", e.getRequest(), serverId);
                            break;
                          default:
                            scheduleNextRunImmediately = false;
                            log.error("Server[%s] returned unknown state in status[%s].", serverId, e.getStatus());
                        }
                      }

                      if (numSuccessfulLoads > 0) {
                        loadingRateTracker.incrementBytesLoadedInBatch(successfulLoadSize);
                      }
                    }
                  }
                  catch (Exception ex) {
                    scheduleNextRunImmediately = false;
                    logRequestFailure(ex);
                  }
                } else {
                  scheduleNextRunImmediately = false;
                  logRequestFailure(new RE("Unexpected Response Status."));
                }
              }
              finally {
                mainLoopInProgress.set(false);

                if (scheduleNextRunImmediately) {
                  processingExecutor.execute(HttpLoadQueuePeon.this::doSegmentManagement);
                }
              }
            }

            @Override
            public void onFailure(Throwable t)
            {
              try {
                logRequestFailure(t);
              }
              finally {
                mainLoopInProgress.set(false);
              }
            }

            private void logRequestFailure(Throwable t)
            {
              log.error(
                  t,
                  "Request[%s] Failed with status[%s]. Reason[%s].",
                  changeRequestURL, responseHandler.getStatus(), responseHandler.getDescription()
              );
            }
          },
          processingExecutor
      );
    }
    catch (Throwable th) {
      log.error(th, "Error sending load/drop request to [%s].", serverId);
      mainLoopInProgress.set(false);
    }
  }

  /**
   * Calculates the number of segments the server is capable of handling at a time. If loading segments in turbo loading
   * mode, returns the number of turbo loading threads on the server. Otherwise, return the value set by the batch size
   * runtime parameter, or number of normal threads on the server if the parameter is not set.
   * Always returns a positive integer.
   */
  @VisibleForTesting
  int calculateBatchSize(SegmentLoadingMode loadingMode)
  {
    final SegmentLoadingCapabilities capabilities = serverCapabilities;
    int batchSize;
    if (SegmentLoadingMode.TURBO.equals(loadingMode)) {
      batchSize = capabilities.getNumTurboLoadingThreads();
    } else {
      batchSize = Configs.valueOrDefault(config.getBatchSize(), capabilities.getNumLoadingThreads());
    }

    return Math.max(batchSize, 1);
  }

  private void handleResponseStatus(DataSegmentChangeRequest changeRequest, SegmentChangeStatus status)
  {
    changeRequest.go(
        new DataSegmentChangeHandler()
        {
          @Override
          public void addSegment(DataSegment segment, DataSegmentChangeCallback callback)
          {
            updateSuccessOrFailureInHolder(segmentsToLoad.remove(segment), status);
          }

          @Override
          public void removeSegment(DataSegment segment, DataSegmentChangeCallback callback)
          {
            updateSuccessOrFailureInHolder(segmentsToDrop.remove(segment), status);
          }

          private void updateSuccessOrFailureInHolder(SegmentHolder holder, SegmentChangeStatus status)
          {
            if (holder == null) {
              return;
            }

            queuedSegments.remove(holder);
            activeRequestSegments.remove(holder.getSegment());
            if (status.getState() == SegmentChangeStatus.State.FAILED) {
              onRequestFailed(holder, status);
            } else {
              onRequestCompleted(holder, RequestStatus.SUCCESS, status);
            }
          }
        }, null
    );
  }

  @Override
  public void start()
  {
    synchronized (lock) {
      if (stopped) {
        throw new ISE("Can't start.");
      }

      ScheduledExecutors.scheduleAtFixedRate(
          processingExecutor,
          config.getRepeatDelay(),
          () -> {
            if (!stopped) {
              doSegmentManagement();
            }

            if (stopped) {
              return ScheduledExecutors.Signal.STOP;
            } else {
              return ScheduledExecutors.Signal.REPEAT;
            }
          }
      );
    }
  }

  @Override
  public void stop()
  {
    synchronized (lock) {
      if (stopped) {
        return;
      }
      stopped = true;

      if (!queuedSegments.isEmpty()) {
        queuedSegments.forEach(
            holder -> onRequestCompleted(
                holder,
                RequestStatus.CANCELLED,
                SegmentChangeStatus.failed("cancelled")
            )
        );
      }

      segmentsToDrop.clear();
      segmentsToLoad.clear();
      queuedSegments.clear();
      activeRequestSegments.clear();
      queuedSize.set(0L);
      loadingRateTracker.stop();
      stats.get().clear();
    }
  }

  @Override
  public void loadSegment(DataSegment segment, SegmentAction action, LoadPeonCallback callback)
  {
    loadSegment(segment, action, null, callback);
  }

  @Override
  public void loadSegment(
      DataSegment segment,
      SegmentAction action,
      @Nullable PartialLoadProfile profile,
      LoadPeonCallback callback
  )
  {
    if (!action.isLoad()) {
      log.warn("Invalid load action[%s] for segment[%s] on server[%s].", action, segment.getId(), serverId);
      return;
    }

    synchronized (lock) {
      if (stopped) {
        log.warn(
            "Server[%s] cannot load segment[%s] because load queue peon is stopped.",
            serverId, segment.getId()
        );
        if (callback != null) {
          callback.execute(false);
        }
        return;
      }

      SegmentHolder holder = segmentsToLoad.get(segment);
      if (holder == null) {
        queuedSize.addAndGet(segment.getSize());
        holder = new SegmentHolder(segment, action, profile, config.getLoadTimeout(), callback);
        segmentsToLoad.put(segment, holder);
        queuedSegments.add(holder);
        processingExecutor.execute(this::doSegmentManagement);
        incrementStat(holder, RequestStatus.ASSIGNED, null);
      } else {
        holder.addCallback(callback);
      }
    }
  }

  @Override
  public void dropSegment(DataSegment segment, LoadPeonCallback callback)
  {
    synchronized (lock) {
      if (stopped) {
        log.warn(
            "Server[%s] cannot drop segment[%s] because load queue peon is stopped.",
            serverId, segment.getId()
        );
        if (callback != null) {
          callback.execute(false);
        }
        return;
      }
      SegmentHolder holder = segmentsToDrop.get(segment);

      if (holder == null) {
        log.trace("Server[%s] to drop segment[%s] queued.", serverId, segment.getId());
        holder = new SegmentHolder(segment, SegmentAction.DROP, config.getLoadTimeout(), callback);
        segmentsToDrop.put(segment, holder);
        queuedSegments.add(holder);
        processingExecutor.execute(this::doSegmentManagement);
        incrementStat(holder, RequestStatus.ASSIGNED, null);
      } else {
        holder.addCallback(callback);
      }
    }
  }

  @Override
  public Set<DataSegment> getSegmentsToLoad()
  {
    return Collections.unmodifiableSet(segmentsToLoad.keySet());
  }

  @Override
  public Set<DataSegment> getSegmentsToDrop()
  {
    return Collections.unmodifiableSet(segmentsToDrop.keySet());
  }

  @Override
  public Set<DataSegment> getTimedOutSegments()
  {
    return Collections.emptySet();
  }

  @Override
  public Set<SegmentHolder> getSegmentsInQueue()
  {
    final Set<SegmentHolder> segmentsInQueue;
    synchronized (lock) {
      segmentsInQueue = new HashSet<>(queuedSegments);
    }
    return segmentsInQueue;
  }

  @Override
  public long getSizeOfSegmentsToLoad()
  {
    return queuedSize.get();
  }

  @Override
  public long getLoadRateKbps()
  {
    return loadingRateTracker.getMovingAverageLoadRateKbps();
  }

  @Override
  public CoordinatorRunStats getAndResetStats()
  {
    return stats.getAndSet(new CoordinatorRunStats());
  }

  @Override
  public void markSegmentToDrop(DataSegment dataSegment)
  {
    segmentsMarkedToDrop.add(dataSegment);
  }

  @Override
  public void unmarkSegmentToDrop(DataSegment dataSegment)
  {
    segmentsMarkedToDrop.remove(dataSegment);
  }

  @Override
  public Set<DataSegment> getSegmentsMarkedToDrop()
  {
    return Collections.unmodifiableSet(segmentsMarkedToDrop);
  }

  private void onRequestFailed(SegmentHolder holder, SegmentChangeStatus status)
  {
    log.error(
        "Server[%s] failed segment[%s] request[%s] with cause [%s].",
        serverId, holder.getSegment().getId(), holder.getAction(), status.getFailureCause()
    );
    onRequestCompleted(holder, RequestStatus.FAILED, status);
  }

  private void onRequestCompleted(SegmentHolder holder, RequestStatus status, SegmentChangeStatus changeStatus)
  {
    final SegmentAction action = holder.getAction();
    log.trace(
        "Server[%s] completed request[%s] on segment[%s] with status[%s].",
        serverId, action, holder.getSegment().getId(), status
    );

    if (holder.isLoad()) {
      queuedSize.addAndGet(-holder.getSegment().getSize());
    }
    incrementStat(holder, status, changeStatus);
    executeCallbacks(holder, status == RequestStatus.SUCCESS);
  }

  private void incrementStat(SegmentHolder holder, RequestStatus status, SegmentChangeStatus changeStatus)
  {
    String description = holder.getAction().name();
    if (changeStatus != null && changeStatus.getLoadingMode() != null) {
      description += ": " + changeStatus.getLoadingMode().name();
    }

    RowKey rowKey = RowKey.with(Dimension.DATASOURCE, holder.getSegment().getDataSource())
                          .and(Dimension.DESCRIPTION, description);
    stats.get().add(status.datasourceStat, rowKey, 1);
  }

  private void executeCallbacks(SegmentHolder holder, boolean success)
  {
    callBackExecutor.execute(() -> {
      for (LoadPeonCallback callback : holder.getCallbacks()) {
        callback.execute(success);
      }
    });
  }

  /**
   * Tries to cancel a load/drop operation. A load/drop request can be cancelled
   * only if it has not already been sent to the corresponding server.
   */
  @Override
  public boolean cancelOperation(DataSegment segment)
  {
    synchronized (lock) {
      if (activeRequestSegments.contains(segment)) {
        return false;
      }

      // Find the action on this segment, if any
      final SegmentHolder holder = segmentsToLoad.containsKey(segment)
                                   ? segmentsToLoad.remove(segment)
                                   : segmentsToDrop.remove(segment);
      if (holder == null) {
        return false;
      }

      queuedSegments.remove(holder);
      onRequestCompleted(holder, RequestStatus.CANCELLED, SegmentChangeStatus.failed("cancelled"));
      return true;
    }
  }

  private enum RequestStatus
  {
    ASSIGNED(Stats.SegmentQueue.ASSIGNED_ACTIONS),
    SUCCESS(Stats.SegmentQueue.COMPLETED_ACTIONS),
    FAILED(Stats.SegmentQueue.FAILED_ACTIONS),
    CANCELLED(Stats.SegmentQueue.CANCELLED_ACTIONS);

    final CoordinatorStat datasourceStat;

    RequestStatus(CoordinatorStat datasourceStat)
    {
      this.datasourceStat = datasourceStat;
    }
  }

}
