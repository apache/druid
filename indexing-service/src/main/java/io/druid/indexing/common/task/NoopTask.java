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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.druid.data.input.FirehoseFactory;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;

import org.joda.time.DateTime;

import java.util.Map;
import java.util.UUID;

/**
 */
public class NoopTask extends AbstractTask
{
  private static final Logger log = new Logger(NoopTask.class);
  private static final int defaultRunTime = 2500;
  private static final int defaultIsReadyTime = 0;
  private static final IsReadyResult defaultIsReadyResult = IsReadyResult.YES;

  enum IsReadyResult
  {
    YES,
    NO,
    EXCEPTION
  }

  @JsonIgnore
  private final long runTime;

  @JsonIgnore
  private final long isReadyTime;

  @JsonIgnore
  private final IsReadyResult isReadyResult;

  @JsonIgnore
  private final FirehoseFactory firehoseFactory;

  @JsonCreator
  public NoopTask(
      @JsonProperty("id") String id,
      @JsonProperty("runTime") long runTime,
      @JsonProperty("isReadyTime") long isReadyTime,
      @JsonProperty("isReadyResult") String isReadyResult,
      @JsonProperty("firehose") FirehoseFactory firehoseFactory,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        id == null ? String.format("noop_%s_%s", new DateTime(), UUID.randomUUID().toString()) : id,
        "none",
        context
    );

    this.runTime = (runTime == 0) ? defaultRunTime : runTime;
    this.isReadyTime = (isReadyTime == 0) ? defaultIsReadyTime : isReadyTime;
    this.isReadyResult = (isReadyResult == null)
                         ? defaultIsReadyResult
                         : IsReadyResult.valueOf(isReadyResult.toUpperCase());
    this.firehoseFactory = firehoseFactory;
  }

  @Override
  public String getType()
  {
    return "noop";
  }

  @JsonProperty
  public long getRunTime()
  {
    return runTime;
  }

  @JsonProperty
  public long getIsReadyTime()
  {
    return isReadyTime;
  }

  @JsonProperty
  public IsReadyResult getIsReadyResult()
  {
    return isReadyResult;
  }

  @JsonProperty("firehose")
  public FirehoseFactory getFirehoseFactory()
  {
    return firehoseFactory;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    switch (isReadyResult) {
      case YES:
        return true;
      case NO:
        return false;
      case EXCEPTION:
        throw new ISE("Not ready. Never will be ready. Go away!");
      default:
        throw new AssertionError("#notreached");
    }
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    if (firehoseFactory != null) {
      log.info("Connecting firehose");
      firehoseFactory.connect(null);
    }

    log.info("Running noop task[%s]", getId());
    log.info("Sleeping for %,d millis.", runTime);
    Thread.sleep(runTime);
    log.info("Woke up!");
    return TaskStatus.success(getId());
  }

  public static NoopTask create()
  {
    return new NoopTask(null, 0, 0, null, null, null);
  }
}
