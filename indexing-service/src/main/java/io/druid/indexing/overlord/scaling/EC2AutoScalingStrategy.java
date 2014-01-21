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

package io.druid.indexing.overlord.scaling;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Placement;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.guice.annotations.Json;
import io.druid.indexing.overlord.setup.EC2NodeData;
import io.druid.indexing.overlord.setup.GalaxyUserData;
import io.druid.indexing.overlord.setup.WorkerSetupData;
import org.apache.commons.codec.binary.Base64;

import javax.annotation.Nullable;
import java.util.List;

/**
 */
public class EC2AutoScalingStrategy implements AutoScalingStrategy
{
  private static final EmittingLogger log = new EmittingLogger(EC2AutoScalingStrategy.class);

  private final ObjectMapper jsonMapper;
  private final AmazonEC2 amazonEC2Client;
  private final SimpleResourceManagementConfig config;
  private final Supplier<WorkerSetupData> workerSetupDataRef;

  @Inject
  public EC2AutoScalingStrategy(
      @Json ObjectMapper jsonMapper,
      AmazonEC2 amazonEC2Client,
      SimpleResourceManagementConfig config,
      Supplier<WorkerSetupData> workerSetupDataRef
  )
  {
    this.jsonMapper = jsonMapper;
    this.amazonEC2Client = amazonEC2Client;
    this.config = config;
    this.workerSetupDataRef = workerSetupDataRef;
  }

  @Override
  public AutoScalingData provision()
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
              .withPlacement(new Placement(setupData.getAvailabilityZone()))
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

      return new AutoScalingData(
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
          )
      );
    }
    catch (Exception e) {
      log.error(e, "Unable to provision any EC2 instances.");
    }

    return null;
  }

  @Override
  public AutoScalingData terminate(List<String> ips)
  {
    if (ips.isEmpty()) {
      return new AutoScalingData(Lists.<String>newArrayList());
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
      return terminateWithIds(
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
      );
    }
    catch (Exception e) {
      log.error(e, "Unable to terminate any instances.");
    }

    return null;
  }

  @Override
  public AutoScalingData terminateWithIds(List<String> ids)
  {
    if (ids.isEmpty()) {
      return new AutoScalingData(Lists.<String>newArrayList());
    }

    try {
      log.info("Terminating instances[%s]", ids);
      amazonEC2Client.terminateInstances(
          new TerminateInstancesRequest(ids)
      );

      return new AutoScalingData(ids);
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

    log.debug("Performing lookup: %s --> %s", ips, retVal);

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

    log.debug("Performing lookup: %s --> %s", nodeIds, retVal);

    return retVal;
  }
}
