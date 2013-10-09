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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.logger.Logger;
import io.druid.data.input.FirehoseFactory;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

/**
 */
public class NoopTask extends AbstractTask
{
  private static final Logger log = new Logger(NoopTask.class);
  private static int defaultRunTime = 2500;

  private final int runTime;
  private final FirehoseFactory firehoseFactory;

  @JsonCreator
  public NoopTask(
      @JsonProperty("id") String id,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("runTime") int runTime,
      @JsonProperty("firehose") FirehoseFactory firehoseFactory
  )
  {
    super(
        id == null ? String.format("noop_%s", new DateTime()) : id,
        "none",
        interval == null ? new Interval(Period.days(1), new DateTime()) : interval
    );

    this.runTime = (runTime == 0) ? defaultRunTime : runTime;

    this.firehoseFactory = firehoseFactory;
  }

  @Override
  public String getType()
  {
    return "noop";
  }

  @JsonProperty("runTime")
  public int getRunTime()
  {
    return runTime;
  }

  @JsonProperty("firehose")
  public FirehoseFactory getFirehoseFactory()
  {
    return firehoseFactory;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    if (firehoseFactory != null) {
      log.info("Connecting firehose");
      firehoseFactory.connect();
    }

    log.info("Running noop task[%s]", getId());
    log.info("Sleeping for %,d millis.", runTime);
    Thread.sleep(runTime);
    log.info("Woke up!");
    return TaskStatus.success(getId());
  }
}
