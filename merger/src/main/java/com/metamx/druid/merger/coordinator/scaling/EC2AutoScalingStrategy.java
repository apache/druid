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
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.druid.merger.coordinator.config.EC2AutoScalingStrategyConfig;
import com.metamx.druid.merger.coordinator.setup.EC2NodeData;
import com.metamx.druid.merger.coordinator.setup.WorkerSetupData;
import com.metamx.druid.merger.coordinator.setup.WorkerSetupManager;
import com.metamx.emitter.EmittingLogger;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.List;

/**
 */
public class EC2AutoScalingStrategy implements ScalingStrategy<Instance>
{
  private static final EmittingLogger log = new EmittingLogger(EC2AutoScalingStrategy.class);

  private final ObjectMapper jsonMapper;
  private final AmazonEC2Client amazonEC2Client;
  private final EC2AutoScalingStrategyConfig config;
  private final WorkerSetupManager workerSetupManager;

  public EC2AutoScalingStrategy(
      ObjectMapper jsonMapper,
      AmazonEC2Client amazonEC2Client,
      EC2AutoScalingStrategyConfig config,
      WorkerSetupManager workerSetupManager
  )
  {
    this.jsonMapper = jsonMapper;
    this.amazonEC2Client = amazonEC2Client;
    this.config = config;
    this.workerSetupManager = workerSetupManager;
  }

  @Override
  public AutoScalingData<Instance> provision()
  {
    try {
      WorkerSetupData setupData = workerSetupManager.getWorkerSetupData();
      if (!(setupData.getNodeData() instanceof EC2NodeData)) {
        throw new ISE("DB misconfiguration! Node data is an instance of [%s]", setupData.getNodeData().getClass());
      }
      EC2NodeData workerConfig = (EC2NodeData) setupData.getNodeData();

      log.info("Creating new instance(s)...");
      RunInstancesResult result = amazonEC2Client.runInstances(
          new RunInstancesRequest(
              workerConfig.getAmiId(),
              workerConfig.getMinInstances(),
              workerConfig.getMaxInstances()
          )
              .withInstanceType(workerConfig.getInstanceType())
              .withUserData(jsonMapper.writeValueAsString(setupData.getUserData()))
      );

      List<String> instanceIds = Lists.transform(
          result.getReservation().getInstances(),
          new Function<Instance, String>()
          {
            @Override
            public String apply(Instance input)
            {
              return input.getInstanceId();
            }
          }
      );

      log.info("Created instances: %s", instanceIds);

      return new AutoScalingData<Instance>(
          Lists.transform(
              result.getReservation().getInstances(),
              new Function<Instance, String>()
              {
                @Override
                public String apply(Instance input)
                {
                  return String.format("%s:%s", input.getPrivateIpAddress(), config.getWorkerPort());
                }
              }
          ),
          result.getReservation().getInstances()
      );
    }
    catch (Exception e) {
      log.error(e, "Unable to provision any EC2 instances.");
    }

    return null;
  }

  @Override
  public AutoScalingData<Instance> terminate(List<String> nodeIds)
  {
    DescribeInstancesResult result = amazonEC2Client.describeInstances(
        new DescribeInstancesRequest()
            .withFilters(
                new Filter("private-ip-address", nodeIds)
            )
    );

    List<Instance> instances = Lists.newArrayList();
    for (Reservation reservation : result.getReservations()) {
      instances.addAll(reservation.getInstances());
    }

    try {
      log.info("Terminating instance[%s]", instances);
      amazonEC2Client.terminateInstances(
          new TerminateInstancesRequest(
              Lists.transform(
                  instances,
                  new Function<Instance, String>()
                  {
                    @Override
                    public String apply(Instance input)
                    {
                      return input.getInstanceId();
                    }
                  }
              )
          )
      );

      return new AutoScalingData<Instance>(
          Lists.transform(
              instances,
              new Function<Instance, String>()
              {
                @Override
                public String apply(Instance input)
                {
                  return String.format("%s:%s", input.getPrivateIpAddress(), config.getWorkerPort());
                }
              }
          ),
          instances
      );
    }
    catch (Exception e) {
      log.error(e, "Unable to terminate any instances.");
    }

    return null;
  }
}
