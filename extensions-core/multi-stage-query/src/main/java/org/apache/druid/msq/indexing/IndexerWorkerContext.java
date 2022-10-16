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

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Injector;
import com.google.inject.Key;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.processor.Bouncer;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.ControllerClient;
import org.apache.druid.msq.exec.TaskDataSegmentProvider;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.exec.WorkerClient;
import org.apache.druid.msq.exec.WorkerContext;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.input.InputSpecs;
import org.apache.druid.msq.kernel.FrameContext;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.rpc.CoordinatorServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocations;
import org.apache.druid.rpc.ServiceLocator;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.rpc.indexing.SpecificTaskRetryPolicy;
import org.apache.druid.rpc.indexing.SpecificTaskServiceLocator;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.realtime.appenderator.UnifiedIndexerAppenderatorsManager;
import org.apache.druid.server.DruidNode;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;

public class IndexerWorkerContext implements WorkerContext
{
  private static final Logger log = new Logger(IndexerWorkerContext.class);
  private static final long FREQUENCY_CHECK_MILLIS = 1000;
  private static final long FREQUENCY_CHECK_JITTER = 30;

  private final TaskToolbox toolbox;
  private final Injector injector;
  private final IndexIO indexIO;
  private final TaskDataSegmentProvider dataSegmentProvider;
  private final ServiceClientFactory clientFactory;

  @GuardedBy("this")
  private OverlordClient overlordClient;

  @GuardedBy("this")
  private ServiceLocator controllerLocator;

  public IndexerWorkerContext(
      final TaskToolbox toolbox,
      final Injector injector,
      final IndexIO indexIO,
      final TaskDataSegmentProvider dataSegmentProvider,
      final ServiceClientFactory clientFactory
  )
  {
    this.toolbox = toolbox;
    this.injector = injector;
    this.indexIO = indexIO;
    this.dataSegmentProvider = dataSegmentProvider;
    this.clientFactory = clientFactory;
  }

  public static IndexerWorkerContext createProductionInstance(final TaskToolbox toolbox, final Injector injector)
  {
    final IndexIO indexIO = injector.getInstance(IndexIO.class);
    final CoordinatorServiceClient coordinatorServiceClient =
        injector.getInstance(CoordinatorServiceClient.class).withRetryPolicy(StandardRetryPolicy.unlimited());
    final SegmentCacheManager segmentCacheManager =
        injector.getInstance(SegmentCacheManagerFactory.class)
                .manufacturate(new File(toolbox.getIndexingTmpDir(), "segment-fetch"));
    final ServiceClientFactory serviceClientFactory =
        injector.getInstance(Key.get(ServiceClientFactory.class, EscalatedGlobal.class));

    return new IndexerWorkerContext(
        toolbox,
        injector,
        indexIO,
        new TaskDataSegmentProvider(coordinatorServiceClient, segmentCacheManager, indexIO),
        serviceClientFactory
    );
  }

  public TaskToolbox toolbox()
  {
    return toolbox;
  }

  @Override
  public ObjectMapper jsonMapper()
  {
    return toolbox.getJsonMapper();
  }

  @Override
  public Injector injector()
  {
    return injector;
  }

  @Override
  public void registerWorker(Worker worker, Closer closer)
  {
    WorkerChatHandler chatHandler = new WorkerChatHandler(toolbox, worker);
    toolbox.getChatHandlerProvider().register(worker.id(), chatHandler, false);
    closer.register(() -> toolbox.getChatHandlerProvider().unregister(worker.id()));
    closer.register(() -> {
      synchronized (this) {
        if (controllerLocator != null) {
          controllerLocator.close();
        }
      }
    });

    // Register the periodic controller checker
    final ExecutorService periodicControllerCheckerExec = Execs.singleThreaded("controller-status-checker-%s");
    closer.register(periodicControllerCheckerExec::shutdownNow);
    final ServiceLocator controllerLocator = makeControllerLocator(worker.task().getControllerTaskId());
    periodicControllerCheckerExec.submit(() -> controllerCheckerRunnable(controllerLocator, worker));
  }

  @VisibleForTesting
  void controllerCheckerRunnable(final ServiceLocator controllerLocator, final Worker worker)
  {
    while (true) {
      // Add some randomness to the frequency of the loop to avoid requests from simultaneously spun up tasks bunching
      // up and stagger them randomly
      long sleepTimeMillis = FREQUENCY_CHECK_MILLIS + ThreadLocalRandom.current().nextLong(
          -FREQUENCY_CHECK_JITTER,
          2 * FREQUENCY_CHECK_JITTER
      );
      final ServiceLocations controllerLocations;
      try {
        controllerLocations = controllerLocator.locate().get();
      }
      catch (Throwable e) {
        // Service locator exceptions are not recoverable.
        log.noStackTrace().warn(
            e,
            "Periodic fetch of controller location encountered an exception. Worker task [%s] will exit.",
            worker.id()
        );
        worker.controllerFailed();
        break;
      }

      if (controllerLocations.isClosed() || controllerLocations.getLocations().isEmpty()) {
        log.warn(
            "Periodic fetch of controller location returned [%s]. Worker task [%s] will exit.",
            controllerLocations,
            worker.id()
        );
        worker.controllerFailed();
        break;
      }

      try {
        Thread.sleep(sleepTimeMillis);
      }
      catch (InterruptedException ignored) {
        // Do nothing: an interrupt means we were shut down. Status checker should exit quietly.
      }
    }
  }

  @Override
  public File tempDir()
  {
    return toolbox.getIndexingTmpDir();
  }

  @Override
  public ControllerClient makeControllerClient(String controllerId)
  {
    final ServiceLocator locator = makeControllerLocator(controllerId);

    return new IndexerControllerClient(
        clientFactory.makeClient(
            controllerId,
            locator,
            new SpecificTaskRetryPolicy(controllerId, StandardRetryPolicy.unlimited())
        ),
        jsonMapper(),
        locator
    );
  }

  @Override
  public WorkerClient makeWorkerClient()
  {
    // Ignore workerId parameter. The workerId is passed into each method of WorkerClient individually.
    return new IndexerWorkerClient(clientFactory, makeOverlordClient(), jsonMapper());
  }

  @Override
  public FrameContext frameContext(QueryDefinition queryDef, int stageNumber)
  {
    final int numWorkersInJvm;

    // Determine the max number of workers in JVM for memory allocations.
    if (toolbox.getAppenderatorsManager() instanceof UnifiedIndexerAppenderatorsManager) {
      // CliIndexer
      numWorkersInJvm = injector.getInstance(WorkerConfig.class).getCapacity();
    } else {
      // CliPeon
      numWorkersInJvm = 1;
    }

    final IntSet inputStageNumbers =
        InputSpecs.getStageNumbers(queryDef.getStageDefinition(stageNumber).getInputSpecs());
    final int numInputWorkers =
        inputStageNumbers.intStream()
                         .map(inputStageNumber -> queryDef.getStageDefinition(inputStageNumber).getMaxWorkerCount())
                         .sum();

    return new IndexerFrameContext(
        this,
        indexIO,
        dataSegmentProvider,
        WorkerMemoryParameters.compute(
            Runtime.getRuntime().maxMemory(),
            numWorkersInJvm,
            processorBouncer().getMaxCount(),
            numInputWorkers
        )
    );
  }

  @Override
  public int threadCount()
  {
    return processorBouncer().getMaxCount();
  }

  @Override
  public DruidNode selfNode()
  {
    return injector.getInstance(Key.get(DruidNode.class, Self.class));
  }

  @Override
  public Bouncer processorBouncer()
  {
    return injector.getInstance(Bouncer.class);
  }

  private synchronized OverlordClient makeOverlordClient()
  {
    if (overlordClient == null) {
      overlordClient = injector.getInstance(OverlordClient.class)
                               .withRetryPolicy(StandardRetryPolicy.unlimited());
    }
    return overlordClient;
  }

  private synchronized ServiceLocator makeControllerLocator(final String controllerId)
  {
    if (controllerLocator == null) {
      controllerLocator = new SpecificTaskServiceLocator(controllerId, makeOverlordClient());
    }

    return controllerLocator;
  }
}
