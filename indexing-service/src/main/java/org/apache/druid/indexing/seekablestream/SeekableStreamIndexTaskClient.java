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

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.indexing.common.IndexTaskClient;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.FullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public abstract class SeekableStreamIndexTaskClient<T1, T2> extends IndexTaskClient
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
      final FullResponseHolder response = submitRequestWithEmptyContent(
          id, HttpMethod.POST, "stop", publish ? "publish=true" : null, true
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
      final FullResponseHolder response = submitRequestWithEmptyContent(id, HttpMethod.POST, "resume", null, true);
      return isSuccess(response);
    }
    catch (NoTaskLocationException | IOException e) {
      log.warn(e, "Exception while stopping task [%s]", id);
      return false;
    }
  }


  public Map<T1, T2> pause(final String id)
  {
    log.debug("Pause task[%s]", id);

    try {
      final FullResponseHolder response = submitRequestWithEmptyContent(
          id,
          HttpMethod.POST,
          "pause",
          null,
          true
      );

      if (response.getStatus().equals(HttpResponseStatus.OK)) {
        log.info("Task [%s] paused successfully", id);
        return deserialize(response.getContent(), constructMapType(Map.class));
      }

      while (true) {
        if (getStatus(id) == SeekableStreamIndexTask.Status.PAUSED) {
          return getCurrentOffsets(id, true);
        }

        final Duration delay = newRetryPolicy().getAndIncrementRetryDelay();
        if (delay == null) {
          log.error("Task [%s] failed to pause, aborting", id);
          throw new ISE("Task [%s] failed to pause, aborting", id);
        } else {
          final long sleepTime = delay.getMillis();
          log.info(
              "Still waiting for task [%s] to pause; will try again in [%s]",
              id,
              new Duration(sleepTime).toString()
          );
          Thread.sleep(sleepTime);
        }
      }
    }
    catch (NoTaskLocationException e) {
      log.error("Exception [%s] while pausing Task [%s]", e.getMessage(), id);
      return ImmutableMap.of();
    }
    catch (IOException | InterruptedException e) {
      throw new RuntimeException(
          StringUtils.format("Exception [%s] while pausing Task [%s]", e.getMessage(), id),
          e
      );
    }
  }

  public SeekableStreamIndexTask.Status getStatus(final String id)
  {
    log.debug("GetStatus task[%s]", id);

    try {
      final FullResponseHolder response = submitRequestWithEmptyContent(id, HttpMethod.GET, "status", null, true);
      return deserialize(response.getContent(), SeekableStreamIndexTask.Status.class);
    }
    catch (NoTaskLocationException e) {
      return SeekableStreamIndexTask.Status.NOT_STARTED;
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
      final FullResponseHolder response = submitRequestWithEmptyContent(id, HttpMethod.GET, "time/start", null, true);
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
      final FullResponseHolder response = submitRequestWithEmptyContent(
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

  public Map<T1, T2> getCurrentOffsets(final String id, final boolean retry)
  {
    log.debug("GetCurrentOffsets task[%s] retry[%s]", id, retry);

    try {
      final FullResponseHolder response = submitRequestWithEmptyContent(
          id,
          HttpMethod.GET,
          "offsets/current",
          null,
          retry
      );
      return deserialize(response.getContent(), constructMapType(Map.class));
    }
    catch (NoTaskLocationException e) {
      return ImmutableMap.of();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public TreeMap<Integer, Map<T1, T2>> getCheckpoints(final String id, final boolean retry)
  {
    log.debug("GetCheckpoints task[%s] retry[%s]", id, retry);
    try {
      final FullResponseHolder response = submitRequestWithEmptyContent(id, HttpMethod.GET, "checkpoints", null, retry);
      return deserialize(
          response.getContent(),
          constructMapType(TreeMap.class)
      );
    }
    catch (NoTaskLocationException e) {
      return new TreeMap<>();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public ListenableFuture<TreeMap<Integer, Map<T1, T2>>> getCheckpointsAsync(
      final String id,
      final boolean retry
  )
  {
    return doAsync(() -> getCheckpoints(id, retry));
  }

  public Map<T1, T2> getEndOffsets(final String id)
  {
    log.debug("GetEndOffsets task[%s]", id);

    try {
      final FullResponseHolder response = submitRequestWithEmptyContent(id, HttpMethod.GET, "offsets/end", null, true);
      return deserialize(response.getContent(), constructMapType(Map.class));
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
      final Map<T1, T2> endOffsets,
      final boolean finalize
  ) throws IOException
  {
    log.debug("SetEndOffsets task[%s] endOffsets[%s] finalize[%s]", id, endOffsets, finalize);

    try {
      final FullResponseHolder response = submitJsonRequest(
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


  public ListenableFuture<Map<T1, T2>> pauseAsync(final String id)
  {
    return doAsync(() -> pause(id));
  }

  public ListenableFuture<Boolean> setEndOffsetsAsync(
      final String id,
      final Map<T1, T2> endOffsets,
      final boolean finalize
  )
  {
    return doAsync(() -> setEndOffsets(id, endOffsets, finalize));
  }

  public ListenableFuture<Map<T1, T2>> getCurrentOffsetsAsync(final String id, final boolean retry)
  {
    return doAsync(() -> getCurrentOffsets(id, retry));
  }

  public ListenableFuture<Map<T1, T2>> getEndOffsetsAsync(final String id)
  {
    return doAsync(() -> getEndOffsets(id));
  }


  public ListenableFuture<Map<String, Object>> getMovingAveragesAsync(final String id)
  {
    return doAsync(() -> getMovingAverages(id));
  }


  public ListenableFuture<SeekableStreamIndexTask.Status> getStatusAsync(final String id)
  {
    return doAsync(() -> getStatus(id));
  }

  protected abstract JavaType constructMapType(Class<? extends Map> mapType);

}

