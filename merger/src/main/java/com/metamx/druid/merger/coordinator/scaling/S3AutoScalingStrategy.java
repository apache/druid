/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.merger.coordinator.scaling;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.common.ISE;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.druid.merger.coordinator.WorkerWrapper;
import com.metamx.druid.merger.coordinator.config.S3AutoScalingStrategyConfig;
import com.metamx.emitter.EmittingLogger;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

/**
 */
public class S3AutoScalingStrategy implements ScalingStrategy
{
  private static final EmittingLogger log = new EmittingLogger(S3AutoScalingStrategy.class);

  private final AmazonEC2Client amazonEC2Client;
  private final S3AutoScalingStrategyConfig config;

  private final Object lock = new Object();

  private volatile String currentlyProvisioning = null;
  private volatile String currentlyTerminating = null;

  public S3AutoScalingStrategy(
      AmazonEC2Client amazonEC2Client,
      S3AutoScalingStrategyConfig config
  )
  {
    this.amazonEC2Client = amazonEC2Client;
    this.config = config;
  }

  @Override
  public void provisionIfNeeded(Map<String, WorkerWrapper> zkWorkers)
  {
    synchronized (lock) {
      if (zkWorkers.containsKey(currentlyProvisioning)) {
        currentlyProvisioning = null;
      }

      if (currentlyProvisioning != null) {
        log.info(
            "[%s] is still provisioning. Wait for it to finish before requesting new worker.",
            currentlyProvisioning
        );
        return;
      }

      Iterable<WorkerWrapper> availableWorkers = FunctionalIterable.create(zkWorkers.values()).filter(
          new Predicate<WorkerWrapper>()
          {
            @Override
            public boolean apply(WorkerWrapper input)
            {
              return !input.isAtCapacity();
            }
          }
      );

      if (Iterables.size(availableWorkers) == 0) {
        try {
          log.info("Creating a new instance");
          RunInstancesResult result = amazonEC2Client.runInstances(
              new RunInstancesRequest(config.getAmiId(), 1, 1)
                  .withInstanceType(InstanceType.fromValue(config.getInstanceType()))
          );

          if (result.getReservation().getInstances().size() != 1) {
            throw new ISE("Created more than one instance, WTF?!");
          }

          Instance instance = result.getReservation().getInstances().get(0);
          log.info("Created instance: %s", instance.getInstanceId());
          log.debug("%s", instance);

          currentlyProvisioning = String.format("%s:%s", instance.getPrivateIpAddress(), config.getWorkerPort());
        }
        catch (Exception e) {
          log.error(e, "Unable to create instance");
          currentlyProvisioning = null;
        }
      }
    }
  }

  @Override
  public Instance terminateIfNeeded(Map<String, WorkerWrapper> zkWorkers)
  {
    synchronized (lock) {
      if (!zkWorkers.containsKey(currentlyTerminating)) {
        currentlyProvisioning = null;
      }

      if (currentlyTerminating != null) {
        log.info("[%s] has not terminated. Wait for it to finish before terminating again.", currentlyTerminating);
        return null;
      }

      MinMaxPriorityQueue<WorkerWrapper> currWorkers = MinMaxPriorityQueue.orderedBy(
          new Comparator<WorkerWrapper>()
          {
            @Override
            public int compare(WorkerWrapper w1, WorkerWrapper w2)
            {
              return w1.getLastCompletedTaskTime().compareTo(w2.getLastCompletedTaskTime());
            }
          }
      ).create(
          zkWorkers.values()
      );

      if (currWorkers.size() <= config.getMinNuMWorkers()) {
        return null;
      }

      WorkerWrapper thatLazyWorker = currWorkers.poll();

      if (System.currentTimeMillis() - thatLazyWorker.getLastCompletedTaskTime().getMillis()
          > config.getMillisToWaitBeforeTerminating()) {
        DescribeInstancesResult result = amazonEC2Client.describeInstances(
            new DescribeInstancesRequest()
                .withFilters(
                    new Filter("private-ip-address", Arrays.asList(thatLazyWorker.getWorker().getIp()))
                )
        );

        if (result.getReservations().size() != 1 || result.getReservations().get(0).getInstances().size() != 1) {
          throw new ISE("More than one node with the same private IP[%s], WTF?!", thatLazyWorker.getWorker().getIp());
        }

        Instance instance = result.getReservations().get(0).getInstances().get(0);

        try {
          log.info("Terminating instance[%s]", instance.getInstanceId());
          amazonEC2Client.terminateInstances(
              new TerminateInstancesRequest(Arrays.asList(instance.getInstanceId()))
          );

          currentlyTerminating = String.format("%s:%s", instance.getPrivateIpAddress(), config.getWorkerPort());

          return instance;
        }
        catch (Exception e) {
          log.error(e, "Unable to terminate instance");
          currentlyTerminating = null;
        }
      }

      return null;
    }
  }
}
