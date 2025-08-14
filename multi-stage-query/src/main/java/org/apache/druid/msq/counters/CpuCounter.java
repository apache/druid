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

package org.apache.druid.msq.counters;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.utils.JvmUtils;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class CpuCounter implements QueryCounter
{
  private final AtomicLong cpuTime = new AtomicLong();
  private final AtomicLong wallTime = new AtomicLong();

  public void accumulate(final long cpu, final long wall)
  {
    cpuTime.addAndGet(cpu);
    wallTime.addAndGet(wall);
  }

  public <E extends Throwable> void run(final Doer<E> doer) throws E
  {
    final long startCpu = JvmUtils.getCurrentThreadCpuTime();
    final long startWall = System.nanoTime();

    try {
      doer.run();
    }
    finally {
      accumulate(
          JvmUtils.getCurrentThreadCpuTime() - startCpu,
          System.nanoTime() - startWall
      );
    }
  }

  public <T, E extends Throwable> T run(final Returner<T, E> returner) throws E
  {
    final long startCpu = JvmUtils.getCurrentThreadCpuTime();
    final long startWall = System.nanoTime();

    try {
      return returner.run();
    }
    finally {
      accumulate(
          JvmUtils.getCurrentThreadCpuTime() - startCpu,
          System.nanoTime() - startWall
      );
    }
  }

  @Override
  public Snapshot snapshot()
  {
    return new Snapshot(cpuTime.get(), wallTime.get());
  }

  @JsonTypeName("cpu")
  public static class Snapshot implements QueryCounterSnapshot
  {
    private final long cpuTime;
    private final long wallTime;

    @JsonCreator
    public Snapshot(
        @JsonProperty("cpu") long cpuTime,
        @JsonProperty("wall") long wallTime
    )
    {
      this.cpuTime = cpuTime;
      this.wallTime = wallTime;
    }

    @JsonProperty("cpu")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public long getCpuTime()
    {
      return cpuTime;
    }

    @JsonProperty("wall")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public long getWallTime()
    {
      return wallTime;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Snapshot snapshot = (Snapshot) o;
      return cpuTime == snapshot.cpuTime && wallTime == snapshot.wallTime;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(cpuTime, wallTime);
    }

    @Override
    public String toString()
    {
      return "CpuCounter.Snapshot{" +
             "cpuTime=" + cpuTime +
             ", wallTime=" + wallTime +
             '}';
    }
  }

  public interface Doer<E extends Throwable>
  {
    void run() throws E;
  }

  public interface Returner<T, E extends Throwable>
  {
    T run() throws E;
  }
}
