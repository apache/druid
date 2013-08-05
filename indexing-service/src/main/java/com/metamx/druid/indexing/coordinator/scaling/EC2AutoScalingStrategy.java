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

package com.metamx.druid.indexing.coordinator.scaling;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.druid.indexing.coordinator.config.EC2AutoScalingStrategyConfig;
import com.metamx.druid.indexing.coordinator.setup.EC2NodeData;
import com.metamx.druid.indexing.coordinator.setup.GalaxyUserData;
import com.metamx.druid.indexing.coordinator.setup.WorkerSetupData;
import com.metamx.emitter.EmittingLogger;
import org.apache.commons.codec.binary.Base64;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class EC2AutoScalingStrategy implements AutoScalingStrategy<Instance>
{
  private static final EmittingLogger log = new EmittingLogger(EC2AutoScalingStrategy.class);

  private final ObjectMapper jsonMapper;
  private final AmazonEC2Client amazonEC2Client;
  private final EC2AutoScalingStrategyConfig config;
  private final AtomicReference<WorkerSetupData> workerSetupDataRef;

  public EC2AutoScalingStrategy(
      ObjectMapper jsonMapper,
      AmazonEC2Client amazonEC2Client,
      EC2AutoScalingStrategyConfig config,
      AtomicReference<WorkerSetupData> workerSetupDataRef
  )
  {
    this.jsonMapper = jsonMapper;
    this.amazonEC2Client = amazonEC2Client;
    this.config = config;
    this.workerSetupDataRef = workerSetupDataRef;
  }

  @Override
  public AutoScalingData<Instance> provision()
  {
    try {
      WorkerSetupData setupData = workerSetupDataRef.get();
      EC2NodeData workerConfig = setupData.getNodeData();

      GalaxyUserData userData = setupData.getUserData();
      if (config.getWorkerVersion() != null) {
        userData = userData.withVersion(config.getWorkerVersion());
      }

      RunInstancesResult result = amazonEC2Client.runInstances(
          new RunInstancesRequest(
              workerConfig.getAmiId(),
              workerConfig.getMinInstances(),
              workerConfig.getMaxInstances()
          )
              .withInstanceType(workerConfig.getInstanceType())
              .withSecurityGroupIds(workerConfig.getSecurityGroupIds())
              .withKeyName(workerConfig.getKeyName())
              .withUserData(
                  Base64.encodeBase64String(
                      jsonMapper.writeValueAsBytes(
                          userData
                      )
                  )
              )
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
                  return input.getInstanceId();
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
  public AutoScalingData<Instance> terminate(List<String> ips)
  {
    if (ips.isEmpty()) {
      return new AutoScalingData<Instance>(Lists.<String>newArrayList(), Lists.<Instance>newArrayList());
    }

    DescribeInstancesResult result = amazonEC2Client.describeInstances(
        new DescribeInstancesRequest()
            .withFilters(
                new Filter("private-ip-address", ips)
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
              ips,
              new Function<String, String>()
              {
                @Override
                public String apply(@Nullable String input)
                {
                  return String.format("%s:%s", input, config.getWorkerPort());
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

  @Override
  public List<String> ipToIdLookup(List<String> ips)
  {
    DescribeInstancesResult result = amazonEC2Client.describeInstances(
        new DescribeInstancesRequest()
            .withFilters(
                new Filter("private-ip-address", ips)
            )
    );

    List<Instance> instances = Lists.newArrayList();
    for (Reservation reservation : result.getReservations()) {
      instances.addAll(reservation.getInstances());
    }

    List<String> retVal = Lists.transform(
        instances,
        new Function<Instance, String>()
        {
          @Override
          public String apply(Instance input)
          {
            return input.getInstanceId();
          }
        }
    );

    log.info("Performing lookup: %s --> %s", ips, retVal);

    return retVal;
  }

  @Override
  public List<String> idToIpLookup(List<String> nodeIds)
  {
    DescribeInstancesResult result = amazonEC2Client.describeInstances(
        new DescribeInstancesRequest()
            .withFilters(
                new Filter("instance-id", nodeIds)
            )
    );

    List<Instance> instances = Lists.newArrayList();
    for (Reservation reservation : result.getReservations()) {
      instances.addAll(reservation.getInstances());
    }

    List<String> retVal = Lists.transform(
        instances,
        new Function<Instance, String>()
        {
          @Override
          public String apply(Instance input)
          {
            return input.getPrivateIpAddress();
          }
        }
    );

    log.info("Performing lookup: %s --> %s", nodeIds, retVal);

    return retVal;
  }
}
