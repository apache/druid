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
import com.google.common.collect.Maps;
import com.metamx.druid.merger.coordinator.WorkerWrapper;
import com.metamx.druid.merger.coordinator.config.S3AutoScalingStrategyConfig;
import com.metamx.druid.merger.worker.Worker;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 */
public class S3AutoScalingStrategyTest
{
  private static final String AMI_ID = "dummy";
  private static final String INSTANCE_ID = "theInstance";

  private AmazonEC2Client amazonEC2Client;
  private RunInstancesResult runInstancesResult;
  private DescribeInstancesResult describeInstancesResult;
  private Reservation reservation;
  private Instance instance;
  private WorkerWrapper worker;
  private S3AutoScalingStrategy strategy;

  @Before
  public void setUp() throws Exception
  {
    amazonEC2Client = EasyMock.createMock(AmazonEC2Client.class);
    runInstancesResult = EasyMock.createMock(RunInstancesResult.class);
    describeInstancesResult = EasyMock.createMock(DescribeInstancesResult.class);
    reservation = EasyMock.createMock(Reservation.class);

    instance = new Instance().withInstanceId(INSTANCE_ID).withLaunchTime(new Date()).withImageId(AMI_ID);

    worker = new WorkerWrapper(
        new Worker("dummyHost", "dummyIP", 2, "0"),
        new ConcurrentSkipListSet<String>(),
        null
    );
    worker.setLastCompletedTaskTime(new DateTime(0));
    strategy = new S3AutoScalingStrategy(
        amazonEC2Client, new S3AutoScalingStrategyConfig()
    {
      @Override
      public String getAmiId()
      {
        return AMI_ID;
      }

      @Override
      public String getWorkerPort()
      {
        return "8080";
      }

      @Override
      public String getInstanceType()
      {
        return "t1.micro";
      }

      @Override
      public long getMillisToWaitBeforeTerminating()
      {
        return 0;
      }

      @Override
      public int getMinNuMWorkers()
      {
        return 0;
      }
    }
    );
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

    Map<String, WorkerWrapper> zkWorkers = Maps.newHashMap();

    zkWorkers.put(worker.getWorker().getHost(), worker);

    worker.getRunningTasks().add("task1");

    Assert.assertFalse(worker.isAtCapacity());

    worker.getRunningTasks().add("task2");

    Assert.assertTrue(worker.isAtCapacity());

    strategy.provisionIfNeeded(zkWorkers);

    worker.getRunningTasks().remove("task1");
    worker.getRunningTasks().remove("task2");

    Instance deleted = strategy.terminateIfNeeded(zkWorkers);

    Assert.assertEquals(deleted.getInstanceId(), INSTANCE_ID);
  }
}
