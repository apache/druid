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

package io.druid.indexing.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.indexing.common.IndexTaskClient;
import io.druid.indexing.common.TaskInfoProvider;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.response.FullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class KafkaIndexTaskClient extends IndexTaskClient
{
  private static final EmittingLogger log = new EmittingLogger(KafkaIndexTaskClient.class);
  private static final TreeMap<Integer, Map<Integer, Long>> EMPTY_TREE_MAP = new TreeMap<>();

  public KafkaIndexTaskClient(
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

  public Map<Integer, Long> pause(final String id)
  {
    return pause(id, 0);
  }

  public Map<Integer, Long> pause(final String id, final long timeout)
  {
    log.debug("Pause task[%s] timeout[%d]", id, timeout);

    try {
      final FullResponseHolder response = submitRequestWithEmptyContent(
          id,
          HttpMethod.POST,
          "pause",
          timeout > 0 ? StringUtils.format("timeout=%d", timeout) : null,
          true
      );

      if (response.getStatus().equals(HttpResponseStatus.OK)) {
        log.info("Task [%s] paused successfully", id);
        return deserialize(response.getContent(), new TypeReference<Map<Integer, Long>>()
        {
        });
      }

      while (true) {
        if (getStatus(id) == KafkaIndexTask.Status.PAUSED) {
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
      log.error("Exception [%s] while pausing Task [%s]", e.getMessage(), id);
      throw Throwables.propagate(e);
    }
  }

  public KafkaIndexTask.Status getStatus(final String id)
  {
    log.debug("GetStatus task[%s]", id);

    try {
      final FullResponseHolder response = submitRequestWithEmptyContent(id, HttpMethod.GET, "status", null, true);
      return deserialize(response.getContent(), KafkaIndexTask.Status.class);
    }
    catch (NoTaskLocationException e) {
      return KafkaIndexTask.Status.NOT_STARTED;
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
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
      throw Throwables.propagate(e);
    }
  }

  public Map<Integer, Long> getCurrentOffsets(final String id, final boolean retry)
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
      return deserialize(response.getContent(), new TypeReference<Map<Integer, Long>>()
      {
      });
    }
    catch (NoTaskLocationException e) {
      return ImmutableMap.of();
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public TreeMap<Integer, Map<Integer, Long>> getCheckpoints(final String id, final boolean retry)
  {
    log.debug("GetCheckpoints task[%s] retry[%s]", id, retry);
    try {
      final FullResponseHolder response = submitRequestWithEmptyContent(id, HttpMethod.GET, "checkpoints", null, retry);
      return deserialize(
          response.getContent(),
          new TypeReference<TreeMap<Integer, Map<Integer, Long>>>()
          {
          }
      );
    }
    catch (NoTaskLocationException e) {
      return EMPTY_TREE_MAP;
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public ListenableFuture<TreeMap<Integer, Map<Integer, Long>>> getCheckpointsAsync(
      final String id,
      final boolean retry
  )
  {
    return doAsync(() -> getCheckpoints(id, retry));
  }

  public Map<Integer, Long> getEndOffsets(final String id)
  {
    log.debug("GetEndOffsets task[%s]", id);

    try {
      final FullResponseHolder response = submitRequestWithEmptyContent(id, HttpMethod.GET, "offsets/end", null, true);
      return deserialize(response.getContent(), new TypeReference<Map<Integer, Long>>()
      {
      });
    }
    catch (NoTaskLocationException e) {
      return ImmutableMap.of();
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public boolean setEndOffsets(
      final String id,
      final Map<Integer, Long> endOffsets,
      final boolean resume,
      final boolean finalize
  )
  {
    log.debug("SetEndOffsets task[%s] endOffsets[%s] resume[%s] finalize[%s]", id, endOffsets, resume, finalize);

    try {
      final FullResponseHolder response = submitJsonRequest(
          id,
          HttpMethod.POST,
          "offsets/end",
          StringUtils.format("resume=%s&finish=%s", resume, finalize),
          serialize(endOffsets),
          true
      );
      return isSuccess(response);
    }
    catch (NoTaskLocationException e) {
      return false;
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
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

  public ListenableFuture<Map<Integer, Long>> pauseAsync(final String id)
  {
    return pauseAsync(id, 0);
  }

  public ListenableFuture<Map<Integer, Long>> pauseAsync(final String id, final long timeout)
  {
    return doAsync(() -> pause(id, timeout));
  }

  public ListenableFuture<KafkaIndexTask.Status> getStatusAsync(final String id)
  {
    return doAsync(() -> getStatus(id));
  }

  public ListenableFuture<DateTime> getStartTimeAsync(final String id)
  {
    return doAsync(() -> getStartTime(id));
  }

  public ListenableFuture<Map<Integer, Long>> getCurrentOffsetsAsync(final String id, final boolean retry)
  {
    return doAsync(() -> getCurrentOffsets(id, retry));
  }

  public ListenableFuture<Map<Integer, Long>> getEndOffsetsAsync(final String id)
  {
    return doAsync(() -> getEndOffsets(id));
  }

  public ListenableFuture<Boolean> setEndOffsetsAsync(
      final String id,
      final Map<Integer, Long> endOffsets,
      final boolean resume,
      final boolean finalize
  )
  {
    return doAsync(() -> setEndOffsets(id, endOffsets, resume, finalize));
  }
}
