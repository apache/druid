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

package io.druid.benchmark;

import com.google.common.base.Supplier;

import io.druid.collections.NonBlockingPool;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
import io.druid.java.util.common.logger.Logger;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@State(Scope.Benchmark)
public class StupidPoolConcurrencyBenchmark
{
  private static final Logger log = new Logger(StupidPoolConcurrencyBenchmark.class);


  @Setup(Level.Iteration)
  public void setup() throws IOException
  {

  }

  @TearDown(Level.Iteration)
  public void teardown()
  {

  }

  private static final Object simpleObject = new Object();

  @State(Scope.Benchmark)
  public static class BenchmarkPool
  {
    private final AtomicLong numPools = new AtomicLong(0L);
    private final NonBlockingPool<Object> pool = new StupidPool<>(
        "simpleObject pool",
        new Supplier<Object>()
        {
          @Override
          public Object get()
          {
            numPools.incrementAndGet();
            return simpleObject;
          }
        }
    );
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void hammerQueue(BenchmarkPool pool, Blackhole blackhole) throws IOException
  {
    try(ResourceHolder<Object> holder = pool.pool.take()){
      blackhole.consume(holder);
    }
  }
}
