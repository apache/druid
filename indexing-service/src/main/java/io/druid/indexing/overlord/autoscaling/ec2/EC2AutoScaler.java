/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.overlord.autoscaling.ec2;

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
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.emitter.EmittingLogger;
import io.druid.indexing.overlord.autoscaling.AutoScaler;
import io.druid.indexing.overlord.autoscaling.AutoScalingData;
import io.druid.indexing.overlord.autoscaling.SimpleResourceManagementConfig;

import java.util.List;

/**
 */
public class EC2AutoScaler implements AutoScaler<EC2EnvironmentConfig>
{
  private static final EmittingLogger log = new EmittingLogger(EC2AutoScaler.class);

  private final int minNumWorkers;
  private final int maxNumWorkers;
  private final EC2EnvironmentConfig envConfig;
  private final AmazonEC2 amazonEC2Client;
  private final SimpleResourceManagementConfig config;

  @JsonCreator
  public EC2AutoScaler(
      @JsonProperty("minNumWorkers") int minNumWorkers,
      @JsonProperty("maxNumWorkers") int maxNumWorkers,
      @JsonProperty("envConfig") EC2EnvironmentConfig envConfig,
      @JacksonInject AmazonEC2 amazonEC2Client,
      @JacksonInject SimpleResourceManagementConfig config
  )
  {
    this.minNumWorkers = minNumWorkers;
    this.maxNumWorkers = maxNumWorkers;
    this.envConfig = envConfig;
    this.amazonEC2Client = amazonEC2Client;
    this.config = config;
  }

  @Override
  @JsonProperty
  public int getMinNumWorkers()
  {
    return minNumWorkers;
  }

  @Override
  @JsonProperty
  public int getMaxNumWorkers()
  {
    return maxNumWorkers;
  }

  @Override
  @JsonProperty
  public EC2EnvironmentConfig getEnvConfig()
  {
    return envConfig;
  }

  @Override
  public AutoScalingData provision()
  {
    try {
      final EC2NodeData workerConfig = envConfig.getNodeData();
      final String userDataBase64;

      if (envConfig.getUserData() == null) {
        userDataBase64 = null;
      } else {
        if (config.getWorkerVersion() == null) {
          userDataBase64 = envConfig.getUserData().getUserDataBase64();
        } else {
          userDataBase64 = envConfig.getUserData()
                                    .withVersion(config.getWorkerVersion())
                                    .getUserDataBase64();
        }
      }

      final RunInstancesResult result = amazonEC2Client.runInstances(
          new RunInstancesRequest(
              workerConfig.getAmiId(),
              workerConfig.getMinInstances(),
              workerConfig.getMaxInstances()
          )
              .withInstanceType(workerConfig.getInstanceType())
              .withSecurityGroupIds(workerConfig.getSecurityGroupIds())
              .withPlacement(new Placement(envConfig.getAvailabilityZone()))
              .withKeyName(workerConfig.getKeyName())
              .withSubnetId(workerConfig.getSubnetId())
              .withUserData(userDataBase64)
      );

      final List<String> instanceIds = Lists.transform(
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

  @Override
  public String toString()
  {
    return "EC2AutoScaler{" +
           "envConfig=" + envConfig +
           ", maxNumWorkers=" + maxNumWorkers +
           ", minNumWorkers=" + minNumWorkers +
           '}';
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

    EC2AutoScaler that = (EC2AutoScaler) o;

    if (maxNumWorkers != that.maxNumWorkers) {
      return false;
    }
    if (minNumWorkers != that.minNumWorkers) {
      return false;
    }
    if (envConfig != null ? !envConfig.equals(that.envConfig) : that.envConfig != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = minNumWorkers;
    result = 31 * result + maxNumWorkers;
    result = 31 * result + (envConfig != null ? envConfig.hashCode() : 0);
    return result;
  }
}
