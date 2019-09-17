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
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public abstract class SeekableStreamIndexTaskClient<PartitionIdType, SequenceOffsetType> extends IndexTaskClient
{
  private static final EmittingLogger log = new EmittingLogger(SeekableStreamIndexTaskClient.class);

  public SeekableStreamIndexTaskClient(
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

  public boolean stop(final String id, final boolean publish)
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

  public boolean resume(final String id)
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

  public Map<PartitionIdType, SequenceOffsetType> pause(final String id)
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
        while (true) {
          final SeekableStreamIndexTaskRunner.Status status = getStatus(id);
          if (status == SeekableStreamIndexTaskRunner.Status.PAUSED) {
            return getCurrentOffsets(id, true);
          }

          final Duration delay = newRetryPolicy().getAndIncrementRetryDelay();
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

  public SeekableStreamIndexTaskRunner.Status getStatus(final String id)
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
  public DateTime getStartTime(final String id)
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

  public Map<String, Object> getMovingAverages(final String id)
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
      return response.getContent() == null || response.getContent().isEmpty()
             ? Collections.emptyMap()
             : deserialize(response.getContent(), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);
    }
    catch (NoTaskLocationException e) {
      return Collections.emptyMap();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Map<PartitionIdType, SequenceOffsetType> getCurrentOffsets(final String id, final boolean retry)
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

  public TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>> getCheckpoints(final String id, final boolean retry)
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

  public ListenableFuture<TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>>> getCheckpointsAsync(
      final String id,
      final boolean retry
  )
  {
    return doAsync(() -> getCheckpoints(id, retry));
  }

  public Map<PartitionIdType, SequenceOffsetType> getEndOffsets(final String id)
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

  public boolean setEndOffsets(
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

  public ListenableFuture<Boolean> stopAsync(final String id, final boolean publish)
  {
    return doAsync(() -> stop(id, publish));
  }


  public ListenableFuture<Boolean> resumeAsync(final String id)
  {
    return doAsync(() -> resume(id));
  }


  public ListenableFuture<DateTime> getStartTimeAsync(final String id)
  {
    return doAsync(() -> getStartTime(id));
  }


  public ListenableFuture<Map<PartitionIdType, SequenceOffsetType>> pauseAsync(final String id)
  {
    return doAsync(() -> pause(id));
  }

  public ListenableFuture<Boolean> setEndOffsetsAsync(
      final String id,
      final Map<PartitionIdType, SequenceOffsetType> endOffsets,
      final boolean finalize
  )
  {
    return doAsync(() -> setEndOffsets(id, endOffsets, finalize));
  }

  public ListenableFuture<Map<PartitionIdType, SequenceOffsetType>> getCurrentOffsetsAsync(
      final String id,
      final boolean retry
  )
  {
    return doAsync(() -> getCurrentOffsets(id, retry));
  }

  public ListenableFuture<Map<PartitionIdType, SequenceOffsetType>> getEndOffsetsAsync(final String id)
  {
    return doAsync(() -> getEndOffsets(id));
  }


  public ListenableFuture<Map<String, Object>> getMovingAveragesAsync(final String id)
  {
    return doAsync(() -> getMovingAverages(id));
  }


  public ListenableFuture<SeekableStreamIndexTaskRunner.Status> getStatusAsync(final String id)
  {
    return doAsync(() -> getStatus(id));
  }

  protected abstract Class<PartitionIdType> getPartitionType();

  protected abstract Class<SequenceOffsetType> getSequenceType();
}


