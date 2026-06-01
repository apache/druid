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

package org.apache.druid.indexing.overlord.autoscaling.ec2;

import com.google.common.base.Functions;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.apache.druid.indexing.overlord.autoscaling.AutoScalingData;
import org.apache.druid.indexing.overlord.autoscaling.SimpleWorkerProvisioningConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 */
public class EC2AutoScalerTest
{
  private static final String AMI_ID = "dummy";
  private static final String INSTANCE_ID = "theInstance";
  public static final EC2EnvironmentConfig ENV_CONFIG = new EC2EnvironmentConfig(
      "us-east-1a",
      new EC2NodeData(AMI_ID, INSTANCE_ID, 1, 1, new ArrayList<>(), "foo", "mySubnet", null, null),
      new GalaxyEC2UserData(new DefaultObjectMapper(), "env", "version", "type")
  );
  private static final String IP = "dummyIP";

  private Ec2Client amazonEC2Client;
  private Instance instance;
  private SimpleWorkerProvisioningConfig managementConfig;

  @Before
  public void setUp()
  {
    amazonEC2Client = EasyMock.createMock(Ec2Client.class);

    instance = Instance.builder()
        .instanceId(INSTANCE_ID)
        .launchTime(Instant.now())
        .imageId(AMI_ID)
        .privateIpAddress(IP)
        .build();

    managementConfig = new SimpleWorkerProvisioningConfig().setWorkerPort(8080).setWorkerVersion("");
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(amazonEC2Client);
  }

  @Test
  public void testScale()
  {
    EC2AutoScaler autoScaler = new EC2AutoScaler(
        0,
        1,
        ENV_CONFIG,
        amazonEC2Client,
        managementConfig
    );

    RunInstancesResponse runInstancesResponse = RunInstancesResponse.builder()
        .instances(Collections.singletonList(instance))
        .build();

    Reservation reservation = Reservation.builder()
        .instances(Collections.singletonList(instance))
        .build();

    DescribeInstancesResponse describeInstancesResponse = DescribeInstancesResponse.builder()
        .reservations(Collections.singletonList(reservation))
        .build();

    EasyMock.expect(amazonEC2Client.runInstances(EasyMock.anyObject(RunInstancesRequest.class)))
            .andReturn(runInstancesResponse);
    EasyMock.expect(amazonEC2Client.describeInstances(EasyMock.anyObject(DescribeInstancesRequest.class)))
            .andReturn(describeInstancesResponse);
    EasyMock.expect(amazonEC2Client.terminateInstances(EasyMock.anyObject(TerminateInstancesRequest.class)))
            .andReturn(null);
    EasyMock.replay(amazonEC2Client);

    AutoScalingData created = autoScaler.provision();

    Assert.assertEquals(created.getNodeIds().size(), 1);
    Assert.assertEquals("theInstance", created.getNodeIds().get(0));

    AutoScalingData deleted = autoScaler.terminate(Collections.singletonList("dummyIP"));

    Assert.assertEquals(deleted.getNodeIds().size(), 1);
    Assert.assertEquals(INSTANCE_ID, deleted.getNodeIds().get(0));
  }

  @Test
  public void testIptoIdLookup()
  {
    EC2AutoScaler autoScaler = new EC2AutoScaler(
        0,
        1,
        ENV_CONFIG,
        amazonEC2Client,
        managementConfig
    );

    final int n = 150;
    Assert.assertTrue(n <= 2 * EC2AutoScaler.MAX_AWS_FILTER_VALUES);

    List<String> ips = Lists.transform(
        ContiguousSet.create(Range.closedOpen(0, n), DiscreteDomain.integers()).asList(),
        Functions.toStringFunction()
    );

    // Create reservations for chunk 1
    List<Reservation> chunk1Reservations = IntStream.range(0, EC2AutoScaler.MAX_AWS_FILTER_VALUES)
        .mapToObj(i -> Reservation.builder()
            .instances(Collections.singletonList(instance))
            .build())
        .collect(Collectors.toList());

    // Create reservations for chunk 2
    List<Reservation> chunk2Reservations = IntStream.range(0, n - EC2AutoScaler.MAX_AWS_FILTER_VALUES)
        .mapToObj(i -> Reservation.builder()
            .instances(Collections.singletonList(instance))
            .build())
        .collect(Collectors.toList());

    DescribeInstancesResponse response1 = DescribeInstancesResponse.builder()
        .reservations(chunk1Reservations)
        .build();

    DescribeInstancesResponse response2 = DescribeInstancesResponse.builder()
        .reservations(chunk2Reservations)
        .build();

    EasyMock.expect(amazonEC2Client.describeInstances(EasyMock.anyObject(DescribeInstancesRequest.class)))
            .andReturn(response1);
    EasyMock.expect(amazonEC2Client.describeInstances(EasyMock.anyObject(DescribeInstancesRequest.class)))
            .andReturn(response2);

    EasyMock.replay(amazonEC2Client);

    List<String> ids = autoScaler.ipToIdLookup(ips);

    Assert.assertEquals(n, ids.size());
  }

  @Test
  public void testIdToIpLookup()
  {
    EC2AutoScaler autoScaler = new EC2AutoScaler(
        0,
        1,
        ENV_CONFIG,
        amazonEC2Client,
        managementConfig
    );

    final int n = 150;
    Assert.assertTrue(n <= 2 * EC2AutoScaler.MAX_AWS_FILTER_VALUES);

    List<String> ids = Lists.transform(
        ContiguousSet.create(Range.closedOpen(0, n), DiscreteDomain.integers()).asList(),
        Functions.toStringFunction()
    );

    // Create reservations for chunk 1
    List<Reservation> chunk1Reservations = IntStream.range(0, EC2AutoScaler.MAX_AWS_FILTER_VALUES)
        .mapToObj(i -> Reservation.builder()
            .instances(Collections.singletonList(instance))
            .build())
        .collect(Collectors.toList());

    // Create reservations for chunk 2
    List<Reservation> chunk2Reservations = IntStream.range(0, n - EC2AutoScaler.MAX_AWS_FILTER_VALUES)
        .mapToObj(i -> Reservation.builder()
            .instances(Collections.singletonList(instance))
            .build())
        .collect(Collectors.toList());

    DescribeInstancesResponse response1 = DescribeInstancesResponse.builder()
        .reservations(chunk1Reservations)
        .build();

    DescribeInstancesResponse response2 = DescribeInstancesResponse.builder()
        .reservations(chunk2Reservations)
        .build();

    EasyMock.expect(amazonEC2Client.describeInstances(EasyMock.anyObject(DescribeInstancesRequest.class)))
            .andReturn(response1);
    EasyMock.expect(amazonEC2Client.describeInstances(EasyMock.anyObject(DescribeInstancesRequest.class)))
            .andReturn(response2);

    EasyMock.replay(amazonEC2Client);

    List<String> resultIps = autoScaler.idToIpLookup(ids);

    Assert.assertEquals(n, resultIps.size());
  }
}
