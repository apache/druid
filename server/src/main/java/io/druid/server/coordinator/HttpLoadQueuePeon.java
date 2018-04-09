/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.io.AppendableByteArrayInputStream;
import io.druid.java.util.http.client.response.ClientResponse;
import io.druid.java.util.http.client.response.InputStreamResponseHandler;
import io.druid.server.coordination.DataSegmentChangeCallback;
import io.druid.server.coordination.DataSegmentChangeHandler;
import io.druid.server.coordination.DataSegmentChangeRequest;
import io.druid.server.coordination.SegmentChangeRequestDrop;
import io.druid.server.coordination.SegmentChangeRequestLoad;
import io.druid.server.coordination.SegmentLoadDropHandler;
import io.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.joda.time.Duration;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
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
public class HttpLoadQueuePeon extends LoadQueuePeon
{
  public static final TypeReference REQUEST_ENTITY_TYPE_REF = new TypeReference<List<DataSegmentChangeRequest>>()
  {
  };

  public static final TypeReference RESPONSE_ENTITY_TYPE_REF = new TypeReference<List<SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus>>()
  {
  };

  private static final EmittingLogger log = new EmittingLogger(HttpLoadQueuePeon.class);

  private final AtomicLong queuedSize = new AtomicLong(0);
  private final AtomicInteger failedAssignCount = new AtomicInteger(0);

  private final ConcurrentSkipListMap<DataSegment, SegmentHolder> segmentsToLoad = new ConcurrentSkipListMap<>(
      DruidCoordinator.SEGMENT_COMPARATOR
  );
  private final ConcurrentSkipListMap<DataSegment, SegmentHolder> segmentsToDrop = new ConcurrentSkipListMap<>(
      DruidCoordinator.SEGMENT_COMPARATOR
  );
  private final ConcurrentSkipListSet<DataSegment> segmentsMarkedToDrop = new ConcurrentSkipListSet<>(
      DruidCoordinator.SEGMENT_COMPARATOR
  );

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
      throw Throwables.propagate(ex);
    }
  }

  private void doSegmentManagement()
  {
    if (stopped || !mainLoopInProgress.compareAndSet(false, true)) {
      log.debug("[%s]Ignoring tick. Either in-progress already or stopped.", serverId);
      return;
    }

    int batchSize = config.getHttpLoadQueuePeonBatchSize();

    List<DataSegmentChangeRequest> newRequests = new ArrayList<>(batchSize);

    synchronized (lock) {
      Iterator<Map.Entry<DataSegment, SegmentHolder>> iter = Iterators.concat(
          segmentsToDrop.entrySet().iterator(),
          segmentsToLoad.entrySet().iterator()
      );

      while (batchSize > 0 && iter.hasNext()) {
        batchSize--;
        Map.Entry<DataSegment, SegmentHolder> entry = iter.next();
        if (entry.getValue().hasTimedOut()) {
          entry.getValue().requestFailed("timed out");
          iter.remove();
        } else {
          newRequests.add(entry.getValue().getChangeRequest());
        }
      }
    }

    if (newRequests.size() == 0) {
      log.debug(
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
      log.debug("Sending [%d] load/drop requests to Server[%s].", newRequests.size(), serverId);
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
                if (responseHandler.status == HttpServletResponse.SC_NO_CONTENT) {
                  log.debug("Received NO CONTENT reseponse from [%s]", serverId);
                } else if (HttpServletResponse.SC_OK == responseHandler.status) {
                  try {
                    List<SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus> statuses = jsonMapper.readValue(
                        result, RESPONSE_ENTITY_TYPE_REF
                    );
                    log.debug("Server[%s] returned status response [%s].", serverId, statuses);
                    synchronized (lock) {
                      if (stopped) {
                        log.debug("Ignoring response from Server[%s]. We are already stopped.", serverId);
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
                            log.info("Request[%s] is still pending on server[%s].", e.getRequest(), serverId);
                            break;
                          default:
                            scheduleNextRunImmediately = false;
                            log.error("WTF! Server[%s] returned unknown state in status[%s].", serverId, e.getStatus());
                        }
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
                responseHandler.description = t.toString();
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
                  responseHandler.status,
                  responseHandler.description
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

          private void updateSuccessOrFailureInHolder(SegmentHolder holder, SegmentLoadDropHandler.Status status)
          {
            if (holder == null) {
              return;
            }

            if (status.getState()
                == SegmentLoadDropHandler.Status.STATE.FAILED) {
              holder.requestFailed(status.getFailureCause());
            } else {
              holder.requestSucceeded();
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
          new Duration(config.getHttpLoadQueuePeonRepeatDelay()),
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

      for (SegmentHolder holder : segmentsToDrop.values()) {
        holder.requestFailed("Stopping load queue peon.");
      }

      for (SegmentHolder holder : segmentsToLoad.values()) {
        holder.requestFailed("Stopping load queue peon.");
      }

      segmentsToDrop.clear();
      segmentsToLoad.clear();
      queuedSize.set(0L);
      failedAssignCount.set(0);
    }
  }

  @Override
  public void loadSegment(DataSegment segment, LoadPeonCallback callback)
  {
    synchronized (lock) {
      if (stopped) {
        log.warn(
            "Server[%s] cannot load segment[%s] because load queue peon is stopped.",
            serverId,
            segment.getIdentifier()
        );
        callback.execute();
        return;
      }

      SegmentHolder holder = segmentsToLoad.get(segment);

      if (holder == null) {
        log.info("Server[%s] to load segment[%s] queued.", serverId, segment.getIdentifier());
        segmentsToLoad.put(segment, new LoadSegmentHolder(segment, callback));
        processingExecutor.execute(this::doSegmentManagement);
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
            serverId,
            segment.getIdentifier()
        );
        callback.execute();
        return;
      }
      SegmentHolder holder = segmentsToDrop.get(segment);

      if (holder == null) {
        log.info("Server[%s] to drop segment[%s] queued.", serverId, segment.getIdentifier());
        segmentsToDrop.put(segment, new DropSegmentHolder(segment, callback));
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

  private abstract class SegmentHolder
  {
    private final DataSegment segment;
    private final DataSegmentChangeRequest changeRequest;
    private final List<LoadPeonCallback> callbacks = Lists.newArrayList();

    // Time when this request was sent to target server the first time.
    private volatile long scheduleTime = -1;

    private SegmentHolder(
        DataSegment segment,
        DataSegmentChangeRequest changeRequest,
        LoadPeonCallback callback
    )
    {
      this.segment = segment;
      this.changeRequest = changeRequest;

      if (callback != null) {
        this.callbacks.add(callback);
      }
    }

    public void addCallback(LoadPeonCallback newCallback)
    {
      synchronized (callbacks) {
        if (newCallback != null) {
          callbacks.add(newCallback);
        }
      }
    }

    public DataSegment getSegment()
    {
      return segment;
    }

    public DataSegmentChangeRequest getChangeRequest()
    {
      return changeRequest;
    }

    public boolean hasTimedOut()
    {
      if (scheduleTime < 0) {
        scheduleTime = System.currentTimeMillis();
        return false;
      } else if (System.currentTimeMillis() - scheduleTime > config.getLoadTimeoutDelay().getMillis()) {
        return true;
      } else {
        return false;
      }
    }

    public void requestSucceeded()
    {
      log.info(
          "Server[%s] Successfully processed segment[%s] request[%s].",
          serverId,
          segment.getIdentifier(),
          changeRequest.getClass().getSimpleName()
      );

      callBackExecutor.execute(() -> {
        for (LoadPeonCallback callback : callbacks) {
          if (callback != null) {
            callback.execute();
          }
        }
      });
    }

    public void requestFailed(String failureCause)
    {
      log.error(
          "Server[%s] Failed segment[%s] request[%s] with cause [%s].",
          serverId,
          segment.getIdentifier(),
          changeRequest.getClass().getSimpleName(),
          failureCause
      );

      failedAssignCount.getAndIncrement();

      callBackExecutor.execute(() -> {
        for (LoadPeonCallback callback : callbacks) {
          if (callback != null) {
            callback.execute();
          }
        }
      });
    }

    @Override
    public String toString()
    {
      return changeRequest.toString();
    }
  }

  private class LoadSegmentHolder extends SegmentHolder
  {
    public LoadSegmentHolder(DataSegment segment, LoadPeonCallback callback)
    {
      super(segment, new SegmentChangeRequestLoad(segment), callback);
      queuedSize.addAndGet(segment.getSize());
    }

    @Override
    public void requestSucceeded()
    {
      queuedSize.addAndGet(-getSegment().getSize());
      super.requestSucceeded();
    }
  }

  private class DropSegmentHolder extends SegmentHolder
  {
    public DropSegmentHolder(DataSegment segment, LoadPeonCallback callback)
    {
      super(segment, new SegmentChangeRequestDrop(segment), callback);
    }
  }

  private static class BytesAccumulatingResponseHandler extends InputStreamResponseHandler
  {
    private int status;
    private String description;

    @Override
    public ClientResponse<AppendableByteArrayInputStream> handleResponse(HttpResponse response)
    {
      status = response.getStatus().getCode();
      description = response.getStatus().getReasonPhrase();
      return ClientResponse.unfinished(super.handleResponse(response).getObj());
    }
  }
}
