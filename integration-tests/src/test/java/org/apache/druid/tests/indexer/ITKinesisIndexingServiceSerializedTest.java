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

package org.apache.druid.tests.indexer;

import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

@Test(groups = TestNGGroup.KINESIS_INDEX)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITKinesisIndexingServiceSerializedTest extends AbstractKinesisIndexingServiceTest
{
  @Override
  public String getTestNamePrefix()
  {
    return "kinesis_serialized";
  }

  @BeforeClass
  public void beforeClass() throws Exception
  {
    doBeforeClass();
  }

  /**
   * This test must be run individually since the test affect and modify the state of the Druid cluster
   */
  @Test
  public void testKinesisIndexDataWithLosingCoordinator() throws Exception
  {
    doTestIndexDataWithLosingCoordinator(null);
  }

  /**
   * This test must be run individually since the test affect and modify the state of the Druid cluster
   */
  @Test
  public void testKinesisIndexDataWithLosingOverlord() throws Exception
  {
    doTestIndexDataWithLosingOverlord(null);
  }

  /**
   * This test must be run individually since the test affect and modify the state of the Druid cluster
   */
  @Test
  public void testKinesisIndexDataWithLosingHistorical() throws Exception
  {
    doTestIndexDataWithLosingHistorical(null);
  }

  /**
   * This test must be run individually due to their resource consumption requirement (task slot, memory, etc.)
   */
  @Test
  public void testKinesisIndexDataWithStartStopSupervisor() throws Exception
  {
    doTestIndexDataWithStartStopSupervisor(null);
  }

  /**
   * This test must be run individually due to their resource consumption requirement (task slot, memory, etc.)
   */
  @Test
  public void testKinesisIndexDataWithKinesisReshardSplit() throws Exception
  {
    doTestIndexDataWithStreamReshardSplit(null);
  }

  /**
   * This test must be run individually due to their resource consumption requirement (task slot, memory, etc.)
   */
  @Test
  public void testKinesisIndexDataWithKinesisReshardMerge() throws Exception
  {
    doTestIndexDataWithStreamReshardMerge();
  }
}
