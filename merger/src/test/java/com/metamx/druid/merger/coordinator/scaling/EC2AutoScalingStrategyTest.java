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
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.google.common.collect.Lists;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.merger.coordinator.config.EC2AutoScalingStrategyConfig;
import com.metamx.druid.merger.coordinator.setup.EC2NodeData;
import com.metamx.druid.merger.coordinator.setup.GalaxyUserData;
import com.metamx.druid.merger.coordinator.setup.WorkerSetupData;
import com.metamx.druid.merger.coordinator.setup.WorkerSetupManager;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;

/**
 */
public class EC2AutoScalingStrategyTest
{
  private static final String AMI_ID = "dummy";
  private static final String INSTANCE_ID = "theInstance";
  private static final String IP = "dummyIP";

  private AmazonEC2Client amazonEC2Client;
  private RunInstancesResult runInstancesResult;
  private DescribeInstancesResult describeInstancesResult;
  private Reservation reservation;
  private Instance instance;
  private EC2AutoScalingStrategy strategy;
  private WorkerSetupManager workerSetupManager;

  @Before
  public void setUp() throws Exception
  {
    amazonEC2Client = EasyMock.createMock(AmazonEC2Client.class);
    runInstancesResult = EasyMock.createMock(RunInstancesResult.class);
    describeInstancesResult = EasyMock.createMock(DescribeInstancesResult.class);
    reservation = EasyMock.createMock(Reservation.class);
    workerSetupManager = EasyMock.createMock(WorkerSetupManager.class);

    instance = new Instance()
        .withInstanceId(INSTANCE_ID)
        .withLaunchTime(new Date())
        .withImageId(AMI_ID)
        .withPrivateIpAddress(IP);

    strategy = new EC2AutoScalingStrategy(
        new DefaultObjectMapper(),
        amazonEC2Client,
        new EC2AutoScalingStrategyConfig()
        {
          @Override
          public String getWorkerPort()
          {
            return "8080";
          }
        },
        workerSetupManager
    );
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(amazonEC2Client);
    EasyMock.verify(runInstancesResult);
    EasyMock.verify(describeInstancesResult);
    EasyMock.verify(reservation);
    EasyMock.verify(workerSetupManager);
  }

  @Test
  public void testScale()
  {
    EasyMock.expect(workerSetupManager.getWorkerSetupData()).andReturn(
        new WorkerSetupData(
            "0",
            0,
            new EC2NodeData(AMI_ID, INSTANCE_ID, 1, 1, Lists.<String>newArrayList(), "foo"),
            new GalaxyUserData("env", "version", "type")
        )
    );
    EasyMock.replay(workerSetupManager);

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

    AutoScalingData created = strategy.provision();

    Assert.assertEquals(created.getNodeIds().size(), 1);
    Assert.assertEquals(created.getNodes().size(), 1);
    Assert.assertEquals("theInstance", created.getNodeIds().get(0));

    AutoScalingData deleted = strategy.terminate(Arrays.asList("dummyIP"));

    Assert.assertEquals(deleted.getNodeIds().size(), 1);
    Assert.assertEquals(deleted.getNodes().size(), 1);
    Assert.assertEquals(String.format("%s:8080", IP), deleted.getNodeIds().get(0));
  }
}
