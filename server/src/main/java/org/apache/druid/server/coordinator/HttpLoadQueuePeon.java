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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
import org.apache.druid.server.coordination.SegmentLoadDropHandler;
import org.apache.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class HttpLoadQueuePeon implements LoadQueuePeon
{
  public static final TypeReference<List<DataSegmentChangeRequest>> REQUEST_ENTITY_TYPE_REF =
      new TypeReference<List<DataSegmentChangeRequest>>()
      {
      };

  public static final TypeReference<List<SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus>> RESPONSE_ENTITY_TYPE_REF =
      new TypeReference<List<SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus>>()
      {
      };

  private static final EmittingLogger log = new EmittingLogger(HttpLoadQueuePeon.class);

  private final AtomicLong queuedSize = new AtomicLong(0);
  private final AtomicInteger failedAssignCount = new AtomicInteger(0);

  private final ConcurrentSkipListMap<DataSegment, QueuedSegment> segmentsToLoad = new ConcurrentSkipListMap<>(
      DruidCoordinator.SEGMENT_COMPARATOR_RECENT_FIRST
  );
  private final ConcurrentSkipListMap<DataSegment, QueuedSegment> segmentsToDrop = new ConcurrentSkipListMap<>(
      DruidCoordinator.SEGMENT_COMPARATOR_RECENT_FIRST
  );
  private final ConcurrentSkipListSet<DataSegment> segmentsMarkedToDrop = new ConcurrentSkipListSet<>(
      DruidCoordinator.SEGMENT_COMPARATOR_RECENT_FIRST
  );
  private final Set<DataSegment> activeRequestSegments = new HashSet<>();

  private final ScheduledExecutorService processingExecutor;

  private volatile boolean stopped = false;

  private final Object lock = new Object();

  private final DruidCoordinatorConfig config;

  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final URL changeRequestURL;
  private final String serverId;

  private final AtomicBoolean mainLoopInProgress = new AtomicBoolean(false);
  private final ExecutorService callBackExecutor;

  private final ObjectWriter requestBodyWriter;

  public HttpLoadQueuePeon(
      String baseUrl,
      ObjectMapper jsonMapper,
      HttpClient httpClient,
      DruidCoordinatorConfig config,
      ScheduledExecutorService processingExecutor,
      ExecutorService callBackExecutor
  )
  {
    this.jsonMapper = jsonMapper;
    this.requestBodyWriter = jsonMapper.writerWithType(REQUEST_ENTITY_TYPE_REF);
    this.httpClient = httpClient;
    this.config = config;
    this.processingExecutor = processingExecutor;
    this.callBackExecutor = callBackExecutor;

    this.serverId = baseUrl;
    try {
      this.changeRequestURL = new URL(
          new URL(baseUrl),
          StringUtils.nonStrictFormat(
              "druid-internal/v1/segments/changeRequests?timeout=%d",
              config.getHttpLoadQueuePeonHostTimeout().getMillis()
          )
      );
    }
    catch (MalformedURLException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void doSegmentManagement()
  {
    if (stopped || !mainLoopInProgress.compareAndSet(false, true)) {
      log.trace("[%s]Ignoring tick. Either in-progress already or stopped.", serverId);
      return;
    }

    final int batchSize = config.getHttpLoadQueuePeonBatchSize();

    final List<DataSegmentChangeRequest> newRequests = new ArrayList<>(batchSize);

    synchronized (lock) {
      Iterator<Map.Entry<DataSegment, QueuedSegment>> iter = Iterators.concat(
          segmentsToDrop.entrySet().iterator(),
          segmentsToLoad.entrySet().iterator()
      );

      activeRequestSegments.clear();
      while (newRequests.size() < batchSize && iter.hasNext()) {
        Map.Entry<DataSegment, QueuedSegment> entry = iter.next();
        QueuedSegment queuedItem = entry.getValue();
        if (hasRequestTimedOut(queuedItem)) {
          onRequestFailed(queuedItem, "timed out");
          iter.remove();
        } else {
          newRequests.add(queuedItem.getChangeRequest());
          activeRequestSegments.add(entry.getKey());
        }
      }
    }

    if (newRequests.size() == 0) {
      log.trace(
          "[%s]Found no load/drop requests. SegmentsToLoad[%d], SegmentsToDrop[%d], batchSize[%d].",
          serverId,
          segmentsToLoad.size(),
          segmentsToDrop.size(),
          config.getHttpLoadQueuePeonBatchSize()
      );
      mainLoopInProgress.set(false);
      return;
    }

    try {
      log.trace("Sending [%d] load/drop requests to Server[%s].", newRequests.size(), serverId);
      BytesAccumulatingResponseHandler responseHandler = new BytesAccumulatingResponseHandler();
      ListenableFuture<InputStream> future = httpClient.go(
          new Request(HttpMethod.POST, changeRequestURL)
              .addHeader(HttpHeaders.Names.ACCEPT, MediaType.APPLICATION_JSON)
              .addHeader(HttpHeaders.Names.CONTENT_TYPE, MediaType.APPLICATION_JSON)
              .setContent(requestBodyWriter.writeValueAsBytes(newRequests)),
          responseHandler,
          new Duration(config.getHttpLoadQueuePeonHostTimeout().getMillis() + 5000)
      );

      Futures.addCallback(
          future,
          new FutureCallback<InputStream>()
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
                    List<SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus> statuses =
                        jsonMapper.readValue(result, RESPONSE_ENTITY_TYPE_REF);
                    log.trace("Server[%s] returned status response [%s].", serverId, statuses);
                    synchronized (lock) {
                      if (stopped) {
                        log.trace("Ignoring response from Server[%s]. We are already stopped.", serverId);
                        scheduleNextRunImmediately = false;
                        return;
                      }

                      for (SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus e : statuses) {
                        switch (e.getStatus().getState()) {
                          case SUCCESS:
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
                      activeRequestSegments.clear();
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
                  changeRequestURL,
                  responseHandler.getStatus(),
                  responseHandler.getDescription()
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

  private void handleResponseStatus(DataSegmentChangeRequest changeRequest, SegmentLoadDropHandler.Status status)
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

          private void updateSuccessOrFailureInHolder(QueuedSegment holder, SegmentLoadDropHandler.Status status)
          {
            if (holder == null) {
              return;
            }

            if (status.getState() == SegmentLoadDropHandler.Status.STATE.FAILED) {
              onRequestFailed(holder, status.getFailureCause());
            } else {
              onRequestSucceeded(holder);
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
          config.getHttpLoadQueuePeonRepeatDelay(),
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

      for (QueuedSegment holder : segmentsToDrop.values()) {
        onRequestFailed(holder, "Stopping load queue peon.");
      }

      for (QueuedSegment holder : segmentsToLoad.values()) {
        onRequestFailed(holder, "Stopping load queue peon.");
      }

      segmentsToDrop.clear();
      segmentsToLoad.clear();
      queuedSize.set(0L);
      failedAssignCount.set(0);
    }
  }

  @Override
  public void loadSegment(DataSegment segment, SegmentAction action, LoadPeonCallback callback)
  {
    Preconditions.checkArgument(action != SegmentAction.DROP);
    synchronized (lock) {
      if (stopped) {
        log.warn(
            "Server[%s] cannot load segment[%s] because load queue peon is stopped.",
            serverId,
            segment.getId()
        );
        callback.execute(false);
        return;
      }

      QueuedSegment holder = segmentsToLoad.get(segment);
      if (holder == null) {
        log.trace("Server[%s] to load segment[%s] queued.", serverId, segment.getId());
        queuedSize.addAndGet(segment.getSize());
        segmentsToLoad.put(segment, new QueuedSegment(segment, action, callback));
        processingExecutor.execute(this::doSegmentManagement);
      } else {
        holder.addCallback(callback);
      }
    }
  }

  @Override
  public void loadSegment(DataSegment segment, LoadPeonCallback callback)
  {
    loadSegment(segment, SegmentAction.LOAD_AS_REPLICA, callback);
  }

  @Override
  public void dropSegment(DataSegment segment, LoadPeonCallback callback)
  {
    synchronized (lock) {
      if (stopped) {
        log.warn(
            "Server[%s] cannot drop segment[%s] because load queue peon is stopped.",
            serverId,
            segment.getId()
        );
        callback.execute(false);
        return;
      }
      QueuedSegment holder = segmentsToDrop.get(segment);

      if (holder == null) {
        log.trace("Server[%s] to drop segment[%s] queued.", serverId, segment.getId());
        segmentsToDrop.put(segment, new QueuedSegment(segment, SegmentAction.DROP, callback));
        processingExecutor.execute(this::doSegmentManagement);
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
  public Map<DataSegment, SegmentAction> getSegmentsInQueue()
  {
    final Map<DataSegment, SegmentAction> segmentsInQueue = new HashMap<>();
    segmentsToLoad.values().forEach(s -> segmentsInQueue.put(s.getSegment(), s.getAction()));
    segmentsToDrop.values().forEach(s -> segmentsInQueue.put(s.getSegment(), s.getAction()));
    return segmentsInQueue;
  }

  @Override
  public long getLoadQueueSize()
  {
    return queuedSize.get();
  }

  @Override
  public int getAndResetFailedAssignCount()
  {
    return failedAssignCount.getAndSet(0);
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
  public int getNumberOfSegmentsInQueue()
  {
    return segmentsToLoad.size();
  }

  @Override
  public Set<DataSegment> getSegmentsMarkedToDrop()
  {
    return Collections.unmodifiableSet(segmentsMarkedToDrop);
  }

  /**
   * A request is considered to have timed out if the time elapsed since it was
   * first sent to the server is greater than the configured load timeout.
   *
   * @see DruidCoordinatorConfig#getLoadTimeoutDelay()
   */
  private boolean hasRequestTimedOut(QueuedSegment holder)
  {
    return System.currentTimeMillis() - holder.getFirstRequestTimeMillis()
           > config.getLoadTimeoutDelay().getMillis();
  }

  private void onRequestSucceeded(QueuedSegment holder)
  {
    log.trace(
        "Server[%s] Successfully processed segment[%s] request[%s].",
        serverId,
        holder.getSegment().getId(),
        holder.getAction()
    );

    if (holder.isLoad()) {
      queuedSize.addAndGet(-holder.getSegment().getSize());
    }
    executeCallbacks(holder, true);
  }

  private void onRequestFailed(QueuedSegment holder, String failureCause)
  {
    log.error(
        "Server[%s] Failed segment[%s] request[%s] with cause [%s].",
        serverId,
        holder.getSegment().getId(),
        holder.getAction(),
        failureCause
    );

    failedAssignCount.getAndIncrement();
    if (holder.isLoad()) {
      queuedSize.addAndGet(-holder.getSegment().getSize());
    }
    executeCallbacks(holder, false);
  }

  private void onRequestCancelled(QueuedSegment holder)
  {
    if (holder.isLoad()) {
      queuedSize.addAndGet(-holder.getSegment().getSize());
    }
    executeCallbacks(holder, false);
  }

  private void executeCallbacks(QueuedSegment holder, boolean success)
  {
    callBackExecutor.execute(() -> {
      for (LoadPeonCallback callback : holder.getCallbacks()) {
        callback.execute(success);
      }
    });
  }

  @Override
  public boolean cancelDrop(DataSegment segment)
  {
    return cancelOperation(segment, false);
  }

  @Override
  public boolean cancelLoad(DataSegment segment)
  {
    return cancelOperation(segment, true);
  }

  /**
   * Tries to cancel a load/drop operation. An load/drop request can be cancelled
   * only if it has not already been sent to the corresponding server.
   */
  private boolean cancelOperation(DataSegment segment, boolean isLoad)
  {
    synchronized (lock) {
      if (activeRequestSegments.contains(segment)) {
        return false;
      }

      final QueuedSegment holder = isLoad ? segmentsToLoad.remove(segment)
                                          : segmentsToDrop.remove(segment);
      if (holder == null) {
        return false;
      }

      onRequestCancelled(holder);
      return true;
    }
  }

}
