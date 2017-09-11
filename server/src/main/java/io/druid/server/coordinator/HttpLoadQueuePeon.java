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
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.RE;
import com.metamx.common.StringUtils;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.io.AppendableByteArrayInputStream;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.InputStreamResponseHandler;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class HttpLoadQueuePeon extends LoadQueuePeon
{
  public static final TypeReference REQUEST_ENTITY_TYPE_REF = new TypeReference<List<DataSegmentChangeRequest>>() {};
  public static final TypeReference RESPONSE_ENTITY_TYPE_REF = new TypeReference<List<Pair<DataSegmentChangeRequest, SegmentLoadDropHandler.Status>>>() {};

  private static final EmittingLogger log = new EmittingLogger(HttpLoadQueuePeon.class);

  private static final int DROP = 0;
  private static final int LOAD = 1;

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
    this.httpClient = httpClient;
    this.config = config;
    this.processingExecutor = processingExecutor;
    this.callBackExecutor = callBackExecutor;

    this.serverId = baseUrl;
    try {
      this.changeRequestURL = new URL(
          new URL(baseUrl),
          StringUtils.safeFormat(
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
      Iterator<Map.Entry<DataSegment, SegmentHolder>> iter = segmentsToDrop.entrySet().iterator();
      while (batchSize-- > 0 && iter.hasNext()) {
        Map.Entry<DataSegment, SegmentHolder> entry = iter.next();
        newRequests.add(entry.getValue().getChangeRequest());
        entry.getValue().scheduleCancellationOnTimeout();
      }

      iter = segmentsToLoad.entrySet().iterator();
      while (batchSize-- > 0 && iter.hasNext()) {
        Map.Entry<DataSegment, SegmentHolder> entry = iter.next();
        newRequests.add(entry.getValue().getChangeRequest());
        entry.getValue().scheduleCancellationOnTimeout();
      }
    }

    if (newRequests.size() > 0) {
      try {
        log.debug("Sending [%d] load/drop requests to Server[%s].", newRequests.size(), serverId);
        BytesAccumulatingResponseHandler responseHandler = new BytesAccumulatingResponseHandler();
        ListenableFuture<InputStream> future = httpClient.go(
            new Request(HttpMethod.POST, changeRequestURL)
                .addHeader(HttpHeaders.Names.ACCEPT, MediaType.APPLICATION_JSON)
                .addHeader(HttpHeaders.Names.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .setContent(jsonMapper.writerWithType(REQUEST_ENTITY_TYPE_REF).writeValueAsBytes(newRequests)),
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

                if (responseHandler.status == HttpServletResponse.SC_NO_CONTENT) {
                  log.debug("Received NO CONTENT reseponse from [%s]", serverId);
                } else if (HttpServletResponse.SC_OK == responseHandler.status) {
                  try {
                    List<Pair<DataSegmentChangeRequest, SegmentLoadDropHandler.Status>> statuses = jsonMapper.readValue(
                        result, RESPONSE_ENTITY_TYPE_REF
                    );

                    synchronized (lock) {
                      if (stopped) {
                        return;
                      }

                      for (Pair<DataSegmentChangeRequest, SegmentLoadDropHandler.Status> e : statuses) {
                        switch (e.rhs.getState()) {
                          case SUCCESS:
                          case FAILED:
                            e.lhs.go(
                                new DataSegmentChangeHandler()
                                {
                                  @Override
                                  public void addSegment(DataSegment segment, DataSegmentChangeCallback callback)
                                  {
                                    SegmentHolder holder = segmentsToLoad.remove(segment);
                                    if (holder == null) {
                                      return;
                                    }

                                    if (e.rhs.getState()
                                        == SegmentLoadDropHandler.Status.STATE.FAILED) {
                                      holder.requestCompleted(
                                          true,
                                          StringUtils.safeFormat(
                                              "Server[%s] Failed segment[%s] LOAD request with cause [%s].",
                                              serverId,
                                              segment.getIdentifier(),
                                              e.rhs.getFailureCause()
                                          )
                                      );
                                    } else {
                                      holder.requestCompleted(false, null);
                                    }

                                  }

                                  @Override
                                  public void removeSegment(DataSegment segment, DataSegmentChangeCallback callback)
                                  {
                                    SegmentHolder holder = segmentsToDrop.remove(segment);
                                    if (holder == null) {
                                      return;
                                    }

                                    if (e.rhs.getState()
                                        == SegmentLoadDropHandler.Status.STATE.FAILED) {
                                      holder.requestCompleted(
                                          true,
                                          StringUtils.safeFormat(
                                              "Server[%s] Failed segment[%s] DROP request with cause [%s].",
                                              serverId,
                                              segment.getIdentifier(),
                                              e.rhs.getFailureCause()
                                          )
                                      );
                                    } else {
                                      holder.requestCompleted(false, null);
                                    }
                                  }
                                }, null
                            );
                            break;
                          case PENDING:
                            log.debug("[%s]Segment request [%s] is pending.", serverId, e.lhs);
                            break;
                          default:
                            scheduleNextRunImmediately = false;
                            log.error("WTF! Server[%s] returned unknown state in status[%s].", serverId, e.rhs);
                        }
                      }
                    }
                  }
                  catch (Exception ex) {
                    scheduleNextRunImmediately = false;
                    onFailure(ex);
                  }
                  finally {
                    mainLoopInProgress.set(false);
                  }
                } else {
                  scheduleNextRunImmediately = false;
                  onFailure(new RE("Unexpected Response Status."));
                }

                if (scheduleNextRunImmediately) {
                  processingExecutor.execute(HttpLoadQueuePeon.this::doSegmentManagement);
                }
              }

              @Override
              public void onFailure(Throwable t)
              {
                try {
                  log.info(
                      "Request[%s] Failed with code[%s] and status[%s]. Reason[%s]. Error [%s]",
                      changeRequestURL,
                      responseHandler.status,
                      responseHandler.description,
                      t.getMessage()
                  );
                  log.error(
                      t,
                      "Request[%s] Failed with code[%s] and status[%s]. Reason[%s].",
                      changeRequestURL,
                      responseHandler.status,
                      responseHandler.description
                  );
                }
                finally {
                  mainLoopInProgress.set(false);
                }
              }
            },
            processingExecutor
        );
      }
      catch (Throwable th) {
        mainLoopInProgress.set(false);
      }
    } else {
      log.debug("[%s] No segments to load/drop.");
      mainLoopInProgress.set(false);
    }
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
          new Callable<ScheduledExecutors.Signal>()
          {
            @Override
            public ScheduledExecutors.Signal call()
            {
              if (!stopped) {
                doSegmentManagement();
              }

              if (stopped) {
                return ScheduledExecutors.Signal.STOP;
              } else {
                return ScheduledExecutors.Signal.REPEAT;
              }
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
        holder.requestCompleted(false, null);
      }

      for (SegmentHolder holder : segmentsToLoad.values()) {
        holder.requestCompleted(false, null);
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
      SegmentHolder holder = segmentsToLoad.get(segment);

      if (holder == null) {
        log.info("Server[%s] to load segment[%s] queued.", serverId, segment.getIdentifier());
        segmentsToLoad.put(segment, new SegmentHolder(segment, LOAD, callback));
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
      SegmentHolder holder = segmentsToDrop.get(segment);

      if (holder == null) {
        log.info("Server[%s] to drop segment[%s] queued.", serverId, segment.getIdentifier());
        segmentsToDrop.put(segment, new SegmentHolder(segment, DROP, callback));
        processingExecutor.execute(this::doSegmentManagement);
      } else {
        holder.addCallback(callback);
      }
    }
  }

  @Override
  public Set<DataSegment> getSegmentsToLoad()
  {
    return segmentsToLoad.keySet();
  }

  @Override
  public Set<DataSegment> getSegmentsToDrop()
  {
    return segmentsToDrop.keySet();
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
    return segmentsMarkedToDrop;
  }

  private class SegmentHolder
  {
    private final DataSegment segment;
    private final DataSegmentChangeRequest changeRequest;
    private final int type;
    private final List<LoadPeonCallback> callbacks = Lists.newArrayList();

    private SegmentHolder(
        DataSegment segment,
        int type,
        LoadPeonCallback callback
    )
    {
      this.segment = segment;
      this.type = type;
      this.changeRequest = (type == LOAD)
                           ? new SegmentChangeRequestLoad(segment)
                           : new SegmentChangeRequestDrop(segment);

      if (callback != null) {
        this.callbacks.add(callback);
      }

      if (type == LOAD) {
        queuedSize.addAndGet(segment.getSize());
      }
    }

    public DataSegment getSegment()
    {
      return segment;
    }

    public int getType()
    {
      return type;
    }

    public void addCallback(LoadPeonCallback newCallback)
    {
      synchronized (callbacks) {
        if (newCallback != null) {
          callbacks.add(newCallback);
        }
      }
    }

    public DataSegmentChangeRequest getChangeRequest()
    {
      return changeRequest;
    }

    AtomicReference<Future> cancellationFutureRef = new AtomicReference<>();

    public void scheduleCancellationOnTimeout()
    {
      cancellationFutureRef.updateAndGet(
          (curr) -> {
            if (curr == null) {
              return processingExecutor.schedule(
                  () -> {
                    synchronized (lock) {
                      if (type == LOAD) {
                        segmentsToLoad.remove(segment);
                      } else {
                        segmentsToDrop.remove(segment);
                      }
                      requestCompleted(
                          true,
                          StringUtils.safeFormat(
                              "Server[%s] failed to complete request[%s] within timeout[%s millis].",
                              serverId,
                              config.getLoadTimeoutDelay().getMillis()
                          )
                      );
                    }
                  },
                  config.getLoadTimeoutDelay().getMillis(),
                  TimeUnit.MILLISECONDS
              );
            } else {
              return curr;
            }
          }
      );
    }

    public void requestCompleted(boolean failed, String failMsg)
    {
      synchronized (lock) {
        Future cancellationFuture = cancellationFutureRef.get();
        
        if (cancellationFuture != null && cancellationFuture.isDone()) {
          return;
        }

        if (cancellationFuture != null) {
          cancellationFuture.cancel(true);
        }

        if (failed) {
          log.error(failMsg);
          failedAssignCount.getAndIncrement();
        }

        if (type == LOAD) {
          queuedSize.addAndGet(-segment.getSize());
        }

        callBackExecutor.execute(() -> {
          for (LoadPeonCallback callback : callbacks) {
            if (callback != null) {
              callback.execute();
            }
          }
        });
      }
    }

    @Override
    public String toString()
    {
      return changeRequest.toString();
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
