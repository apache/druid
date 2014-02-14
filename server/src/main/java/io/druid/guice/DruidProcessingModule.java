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

package io.druid.guice;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.ProvisionException;
import com.metamx.common.concurrent.ExecutorServiceConfig;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.StupidPool;
import io.druid.concurrent.Execs;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Processing;
import io.druid.query.MetricsEmittingExecutorService;
import io.druid.query.PrioritizedExecutorService;
import io.druid.server.DruidProcessingConfig;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class DruidProcessingModule implements Module
{
  private static final Logger log = new Logger(DruidProcessingModule.class);

  @Override
  public void configure(Binder binder)
  {
    ConfigProvider.bind(binder, DruidProcessingConfig.class, ImmutableMap.of("base_path", "druid.processing"));
    binder.bind(ExecutorServiceConfig.class).to(DruidProcessingConfig.class);
  }

  @Provides
  @Processing
  @ManageLifecycle
  public ExecutorService getProcessingExecutorService(
      ExecutorServiceConfig config,
      ServiceEmitter emitter,
      Lifecycle lifecycle
  )
  {
    return new MetricsEmittingExecutorService(
        PrioritizedExecutorService.create(
            lifecycle,
            config
        ),
        emitter,
        new ServiceMetricEvent.Builder()
    );
  }

  @Provides
  @LazySingleton
  @Global
  public StupidPool<ByteBuffer> getIntermediateResultsPool(DruidProcessingConfig config)
  {
    try {
      Class<?> vmClass = Class.forName("sun.misc.VM");
      Object maxDirectMemoryObj = vmClass.getMethod("maxDirectMemory").invoke(null);

      if (maxDirectMemoryObj == null || !(maxDirectMemoryObj instanceof Number)) {
        log.info("Cannot determine maxDirectMemory from[%s]", maxDirectMemoryObj);
      } else {
        long maxDirectMemory = ((Number) maxDirectMemoryObj).longValue();

        final long memoryNeeded = (long) config.intermediateComputeSizeBytes() * (config.getNumThreads() + 1);
        if (maxDirectMemory < memoryNeeded) {
          throw new ProvisionException(
              String.format(
                  "Not enough direct memory.  Please adjust -XX:MaxDirectMemorySize or druid.computation.buffer.size: "
                  + "maxDirectMemory[%,d], memoryNeeded[%,d], druid.computation.buffer.size[%,d], numThreads[%,d]",
                  maxDirectMemory, memoryNeeded, config.intermediateComputeSizeBytes(), config.getNumThreads()
              )
          );
        }
      }
    }
    catch (ClassNotFoundException e) {
      log.info("No VM class, cannot do memory check.");
    }
    catch (NoSuchMethodException e) {
      log.info("VM.maxDirectMemory doesn't exist, cannot do memory check.");
    }
    catch (InvocationTargetException e) {
      log.warn(e, "static method shouldn't throw this");
    }
    catch (IllegalAccessException e) {
      log.warn(e, "public method, shouldn't throw this");
    }

    return new IntermediateProcessingBufferPool(config.intermediateComputeSizeBytes());
  }

  private static class IntermediateProcessingBufferPool extends StupidPool<ByteBuffer>
  {
    private static final Logger log = new Logger(IntermediateProcessingBufferPool.class);

    public IntermediateProcessingBufferPool(final int computationBufferSize)
    {
      super(
          new Supplier<ByteBuffer>()
          {
            final AtomicLong count = new AtomicLong(0);

            @Override
            public ByteBuffer get()
            {
              log.info(
                  "Allocating new intermediate processing buffer[%,d] of size[%,d]",
                  count.getAndIncrement(), computationBufferSize
              );
              return ByteBuffer.allocateDirect(computationBufferSize);
            }
          }
      );
    }
  }
}
