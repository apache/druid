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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.indexing.common.IndexTaskClient;
import org.apache.druid.indexing.common.RetryPolicy;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.segment.incremental.ParseExceptionReport;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Implementation of {@link SeekableStreamIndexTaskClient} based on {@link IndexTaskClient}.
 *
 * Communication is inherently synchronous. Async operations are enabled by scheduling blocking operations on
 * a thread pool.
 *
 * Used when {@link org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorTuningConfig#getChatAsync()}
 * is false.
 */
public abstract class SeekableStreamIndexTaskClientSyncImpl<PartitionIdType, SequenceOffsetType>
    extends IndexTaskClient
    implements SeekableStreamIndexTaskClient<PartitionIdType, SequenceOffsetType>
{
  private static final EmittingLogger log = new EmittingLogger(SeekableStreamIndexTaskClient.class);

  public SeekableStreamIndexTaskClientSyncImpl(
      HttpClient httpClient,
      ObjectMapper jsonMapper,
      TaskInfoProvider taskInfoProvider,
      String dataSource,
      int numThreads,
      Duration httpTimeout,
      long numRetries
  )
  {
    super(httpClient, jsonMapper, taskInfoProvider, httpTimeout, dataSource, numThreads, numRetries);
  }

  private boolean stop(final String id, final boolean publish)
  {
    log.debug("Stop task[%s] publish[%s]", id, publish);

    try {
      final StringFullResponseHolder response = submitRequestWithEmptyContent(
          id,
          HttpMethod.POST,
          "stop",
          publish ? "publish=true" : null,
          true
      );
      return isSuccess(response);
    }
    catch (NoTaskLocationException e) {
      return false;
    }
    catch (TaskNotRunnableException e) {
      log.info("Task [%s] couldn't be stopped because it is no longer running", id);
      return true;
    }
    catch (Exception e) {
      log.warn(e, "Exception while stopping task [%s]", id);
      return false;
    }
  }

  private boolean resume(final String id)
  {
    log.debug("Resume task[%s]", id);

    try {
      final StringFullResponseHolder response = submitRequestWithEmptyContent(id, HttpMethod.POST, "resume", null, true);
      return isSuccess(response);
    }
    catch (NoTaskLocationException | IOException e) {
      log.warn(e, "Exception while stopping task [%s]", id);
      return false;
    }
  }

  private Map<PartitionIdType, SequenceOffsetType> pause(final String id)
  {
    log.debug("Pause task[%s]", id);

    try {
      final StringFullResponseHolder response = submitRequestWithEmptyContent(
          id,
          HttpMethod.POST,
          "pause",
          null,
          true
      );

      final HttpResponseStatus responseStatus = response.getStatus();
      final String responseContent = response.getContent();

      if (responseStatus.equals(HttpResponseStatus.OK)) {
        log.info("Task [%s] paused successfully", id);
        return deserializeMap(responseContent, Map.class, getPartitionType(), getSequenceType());
      } else if (responseStatus.equals(HttpResponseStatus.ACCEPTED)) {
        // The task received the pause request, but its status hasn't been changed yet.
        final RetryPolicy retryPolicy = newRetryPolicy();
        while (true) {
          final SeekableStreamIndexTaskRunner.Status status = getStatus(id);
          if (status == SeekableStreamIndexTaskRunner.Status.PAUSED) {
            return getCurrentOffsets(id, true);
          }

          final Duration delay = retryPolicy.getAndIncrementRetryDelay();
          if (delay == null) {
            throw new ISE(
                "Task [%s] failed to change its status from [%s] to [%s], aborting",
                id,
                status,
                SeekableStreamIndexTaskRunner.Status.PAUSED
            );
          } else {
            final long sleepTime = delay.getMillis();
            log.info(
                "Still waiting for task [%s] to change its status to [%s]; will try again in [%s]",
                id,
                SeekableStreamIndexTaskRunner.Status.PAUSED,
                new Duration(sleepTime).toString()
            );
            Thread.sleep(sleepTime);
          }
        }
      } else {
        throw new ISE(
            "Pause request for task [%s] failed with response [%s] : [%s]",
            id,
            responseStatus,
            responseContent
        );
      }
    }
    catch (NoTaskLocationException e) {
      log.error("Exception [%s] while pausing Task [%s]", e.getMessage(), id);
      return ImmutableMap.of();
    }
    catch (IOException | InterruptedException e) {
      throw new RE(e, "Exception [%s] while pausing Task [%s]", e.getMessage(), id);
    }
  }

  private SeekableStreamIndexTaskRunner.Status getStatus(final String id)
  {
    log.debug("GetStatus task[%s]", id);

    try {
      final StringFullResponseHolder response = submitRequestWithEmptyContent(id, HttpMethod.GET, "status", null, true);
      return deserialize(response.getContent(), SeekableStreamIndexTaskRunner.Status.class);
    }
    catch (NoTaskLocationException e) {
      return SeekableStreamIndexTaskRunner.Status.NOT_STARTED;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Nullable
  private DateTime getStartTime(final String id)
  {
    log.debug("GetStartTime task[%s]", id);

    try {
      final StringFullResponseHolder response = submitRequestWithEmptyContent(id, HttpMethod.GET, "time/start", null, true);
      return response.getContent() == null || response.getContent().isEmpty()
             ? null
             : deserialize(response.getContent(), DateTime.class);
    }
    catch (NoTaskLocationException e) {
      return null;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, Object> getMovingAverages(final String id)
  {
    log.debug("GetMovingAverages task[%s]", id);

    try {
      final StringFullResponseHolder response = submitRequestWithEmptyContent(
          id,
          HttpMethod.GET,
          "rowStats",
          null,
          true
      );
      if (response.getContent() == null || response.getContent().isEmpty()) {
        log.warn("Got empty response when calling getMovingAverages, id[%s]", id);
        return Collections.emptyMap();
      }

      return deserialize(response.getContent(), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);
    }
    catch (NoTaskLocationException e) {
      log.warn(e, "Got NoTaskLocationException when calling getMovingAverages, id[%s]", id);
      return Collections.emptyMap();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private List<ParseExceptionReport> getParseErrors(final String id)
  {
    log.debug("getParseErrors task[%s]", id);

    try {
      final StringFullResponseHolder response = submitRequestWithEmptyContent(
          id,
          HttpMethod.GET,
          "unparseableEvents",
          null,
          true
      );

      if (response.getContent() == null || response.getContent().isEmpty()) {
        log.warn("Got empty response when calling getParseErrors, id[%s]", id);
        return Collections.emptyList();
      }

      return deserialize(response.getContent(), TYPE_REFERENCE_LIST_PARSE_EXCEPTION_REPORT);
    }
    catch (NoTaskLocationException e) {
      log.warn(e, "Got NoTaskLocationException when calling getParseErrors, id[%s]", id);
      return Collections.emptyList();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Map<PartitionIdType, SequenceOffsetType> getCurrentOffsets(final String id, final boolean retry)
  {
    log.debug("GetCurrentOffsets task[%s] retry[%s]", id, retry);

    try {
      final StringFullResponseHolder response = submitRequestWithEmptyContent(
          id,
          HttpMethod.GET,
          "offsets/current",
          null,
          retry
      );
      return deserializeMap(response.getContent(), Map.class, getPartitionType(), getSequenceType());
    }
    catch (NoTaskLocationException e) {
      return ImmutableMap.of();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>> getCheckpoints(final String id, final boolean retry)
  {
    log.debug("GetCheckpoints task[%s] retry[%s]", id, retry);
    try {
      final StringFullResponseHolder response = submitRequestWithEmptyContent(id, HttpMethod.GET, "checkpoints", null, retry);
      return deserializeNestedValueMap(
          response.getContent(),
          TreeMap.class,
          Integer.class,
          Map.class,
          getPartitionType(),
          getSequenceType()
      );
    }
    catch (NoTaskLocationException e) {
      return new TreeMap<>();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ListenableFuture<TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>>> getCheckpointsAsync(
      final String id,
      final boolean retry
  )
  {
    return doAsync(() -> getCheckpoints(id, retry));
  }

  private Map<PartitionIdType, SequenceOffsetType> getEndOffsets(final String id)
  {
    log.debug("GetEndOffsets task[%s]", id);

    try {
      final StringFullResponseHolder response = submitRequestWithEmptyContent(id, HttpMethod.GET, "offsets/end", null, true);
      return deserializeMap(response.getContent(), Map.class, getPartitionType(), getSequenceType());
    }
    catch (NoTaskLocationException e) {
      return ImmutableMap.of();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean setEndOffsets(
      final String id,
      final Map<PartitionIdType, SequenceOffsetType> endOffsets,
      final boolean finalize
  ) throws IOException
  {
    log.debug("SetEndOffsets task[%s] endOffsets[%s] finalize[%s]", id, endOffsets, finalize);

    try {
      final StringFullResponseHolder response = submitJsonRequest(
          id,
          HttpMethod.POST,
          "offsets/end",
          StringUtils.format("finish=%s", finalize),
          serialize(endOffsets),
          true
      );
      return isSuccess(response);
    }
    catch (NoTaskLocationException e) {
      return false;
    }
  }

  @Override
  public ListenableFuture<Boolean> stopAsync(final String id, final boolean publish)
  {
    return doAsync(() -> stop(id, publish));
  }

  @Override
  public ListenableFuture<Boolean> resumeAsync(final String id)
  {
    return doAsync(() -> resume(id));
  }

  @Override
  public ListenableFuture<DateTime> getStartTimeAsync(final String id)
  {
    return doAsync(() -> getStartTime(id));
  }

  @Override
  public ListenableFuture<Map<PartitionIdType, SequenceOffsetType>> pauseAsync(final String id)
  {
    return doAsync(() -> pause(id));
  }

  @Override
  public ListenableFuture<Boolean> setEndOffsetsAsync(
      final String id,
      final Map<PartitionIdType, SequenceOffsetType> endOffsets,
      final boolean finalize
  )
  {
    return doAsync(() -> setEndOffsets(id, endOffsets, finalize));
  }

  @Override
  public ListenableFuture<Map<PartitionIdType, SequenceOffsetType>> getCurrentOffsetsAsync(
      final String id,
      final boolean retry
  )
  {
    return doAsync(() -> getCurrentOffsets(id, retry));
  }

  @Override
  public ListenableFuture<Map<PartitionIdType, SequenceOffsetType>> getEndOffsetsAsync(final String id)
  {
    return doAsync(() -> getEndOffsets(id));
  }

  @Override
  public ListenableFuture<Map<String, Object>> getMovingAveragesAsync(final String id)
  {
    return doAsync(() -> getMovingAverages(id));
  }

  @Override
  public ListenableFuture<List<ParseExceptionReport>> getParseErrorsAsync(final String id)
  {
    return doAsync(() -> getParseErrors(id));
  }

  @Override
  public ListenableFuture<SeekableStreamIndexTaskRunner.Status> getStatusAsync(final String id)
  {
    return doAsync(() -> getStatus(id));
  }
}
