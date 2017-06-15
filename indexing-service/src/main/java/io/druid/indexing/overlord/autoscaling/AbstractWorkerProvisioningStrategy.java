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

package io.druid.indexing.overlord.autoscaling;

import com.google.common.base.Supplier;
import com.metamx.emitter.EmittingLogger;
import io.druid.indexing.overlord.WorkerTaskRunner;
import io.druid.java.util.common.granularity.PeriodGranularity;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 */
public abstract class AbstractWorkerProvisioningStrategy implements ProvisioningStrategy<WorkerTaskRunner>
{
  private static final EmittingLogger log = new EmittingLogger(AbstractWorkerProvisioningStrategy.class);

  private final ProvisioningSchedulerConfig provisioningSchedulerConfig;
  private final Supplier<ScheduledExecutorService> execFactory;

  AbstractWorkerProvisioningStrategy(
      ProvisioningSchedulerConfig provisioningSchedulerConfig,
      Supplier<ScheduledExecutorService> execFactory
  )
  {
    this.provisioningSchedulerConfig = provisioningSchedulerConfig;
    this.execFactory = execFactory;
  }

  @Override
  public ProvisioningService makeProvisioningService(WorkerTaskRunner runner)
  {
    return new WorkerProvisioningService(makeProvisioner(runner));
  }

  final class WorkerProvisioningService implements ProvisioningService
  {
    private final ScheduledExecutorService exec = execFactory.get();
    private final Provisioner provisioner;

    WorkerProvisioningService(final Provisioner provisioner)
    {
      log.info("Started Resource Management Scheduler");
      this.provisioner = provisioner;

      long rate = provisioningSchedulerConfig.getProvisionPeriod().toStandardDuration().getMillis();
      exec.scheduleAtFixedRate(
          new Runnable()
          {
            @Override
            public void run()
            {
              try {
                provisioner.doProvision();
              }
              catch (Exception e) {
                log.error(e, "Uncaught exception.");
              }
            }
          },
          rate,
          rate,
          TimeUnit.MILLISECONDS
      );

      // Schedule termination of worker nodes periodically
      Period period = provisioningSchedulerConfig.getTerminatePeriod();
      PeriodGranularity granularity = new PeriodGranularity(
          period,
          provisioningSchedulerConfig.getOriginTime(),
          null
      );
      final long startTime = granularity.bucketEnd(new DateTime()).getMillis();

      exec.scheduleAtFixedRate(
          new Runnable()
          {
            @Override
            public void run()
            {
              try {
                provisioner.doTerminate();
              }
              catch (Exception e) {
                log.error(e, "Uncaught exception.");
              }
            }
          },
          new Duration(System.currentTimeMillis(), startTime).getMillis(),
          provisioningSchedulerConfig.getTerminatePeriod().toStandardDuration().getMillis(),
          TimeUnit.MILLISECONDS
      );
    }

    @Override
    public ScalingStats getStats()
    {
      return provisioner.getStats();
    }

    @Override
    public void close()
    {
      log.info("Stopping Resource Management Scheduler");
      exec.shutdown();
    }
  }

  protected abstract Provisioner makeProvisioner(WorkerTaskRunner runner);
}
