/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.data.input.FirehoseFactory;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.TaskActionClient;
import org.joda.time.DateTime;

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
      @JsonProperty("firehose") FirehoseFactory firehoseFactory
  )
  {
    super(
        id == null ? String.format("noop_%s", new DateTime()) : id,
        "none"
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
    return new NoopTask(null, 0, 0, null, null);
  }
}
