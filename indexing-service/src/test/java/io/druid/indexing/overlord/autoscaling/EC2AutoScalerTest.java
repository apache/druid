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

package io.druid.indexing.overlord.autoscaling;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.google.common.collect.Lists;
import io.druid.indexing.overlord.autoscaling.ec2.EC2AutoScaler;
import io.druid.indexing.overlord.autoscaling.ec2.EC2EnvironmentConfig;
import io.druid.indexing.overlord.autoscaling.ec2.EC2NodeData;
import io.druid.indexing.overlord.autoscaling.ec2.GalaxyEC2UserData;
import io.druid.jackson.DefaultObjectMapper;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;

/**
 */
public class EC2AutoScalerTest
{
  private static final String AMI_ID = "dummy";
  private static final String INSTANCE_ID = "theInstance";
  private static final String IP = "dummyIP";

  private AmazonEC2Client amazonEC2Client;
  private RunInstancesResult runInstancesResult;
  private DescribeInstancesResult describeInstancesResult;
  private Reservation reservation;
  private Instance instance;
  private SimpleResourceManagementConfig managementConfig;

  @Before
  public void setUp() throws Exception
  {
    amazonEC2Client = EasyMock.createMock(AmazonEC2Client.class);
    runInstancesResult = EasyMock.createMock(RunInstancesResult.class);
    describeInstancesResult = EasyMock.createMock(DescribeInstancesResult.class);
    reservation = EasyMock.createMock(Reservation.class);

    instance = new Instance()
        .withInstanceId(INSTANCE_ID)
        .withLaunchTime(new Date())
        .withImageId(AMI_ID)
        .withPrivateIpAddress(IP);

    managementConfig = new SimpleResourceManagementConfig().setWorkerPort(8080).setWorkerVersion("");
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(amazonEC2Client);
    EasyMock.verify(runInstancesResult);
    EasyMock.verify(describeInstancesResult);
    EasyMock.verify(reservation);
  }

  @Test
  public void testScale()
  {
    EC2AutoScaler autoScaler = new EC2AutoScaler(
        0,
        1,
        new EC2EnvironmentConfig(
            "us-east-1a",
            new EC2NodeData(AMI_ID, INSTANCE_ID, 1, 1, Lists.<String>newArrayList(), "foo"),
                        new GalaxyEC2UserData(new DefaultObjectMapper(), "env", "version", "type")
        ),
        amazonEC2Client,
        managementConfig
    );

    EasyMock.expect(amazonEC2Client.runInstances(EasyMock.anyObject(RunInstancesRequest.class))).andReturn(
        runInstancesResult
    );
    EasyMock.expect(amazonEC2Client.describeInstances(EasyMock.anyObject(DescribeInstancesRequest.class)))
            .andReturn(describeInstancesResult);
    EasyMock.expect(amazonEC2Client.terminateInstances(EasyMock.anyObject(TerminateInstancesRequest.class)))
            .andReturn(null);
    EasyMock.replay(amazonEC2Client);

    EasyMock.expect(runInstancesResult.getReservation()).andReturn(reservation).atLeastOnce();
    EasyMock.replay(runInstancesResult);

    EasyMock.expect(describeInstancesResult.getReservations()).andReturn(Arrays.asList(reservation)).atLeastOnce();
    EasyMock.replay(describeInstancesResult);

    EasyMock.expect(reservation.getInstances()).andReturn(Arrays.asList(instance)).atLeastOnce();
    EasyMock.replay(reservation);

    AutoScalingData created = autoScaler.provision();

    Assert.assertEquals(created.getNodeIds().size(), 1);
    Assert.assertEquals("theInstance", created.getNodeIds().get(0));

    AutoScalingData deleted = autoScaler.terminate(Arrays.asList("dummyIP"));

    Assert.assertEquals(deleted.getNodeIds().size(), 1);
    Assert.assertEquals(INSTANCE_ID, deleted.getNodeIds().get(0));
  }
}
