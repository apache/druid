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

package org.apache.druid.tests.parallelized;

import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.tests.indexer.AbstractKafkaIndexingServiceTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

@Test(groups = TestNGGroup.TRANSACTIONAL_KAFKA_INDEX)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITKafkaIndexingServiceTransactionalParallelizedTest extends AbstractKafkaIndexingServiceTest
{
  @Override
  public String getTestNamePrefix()
  {
    return "kafka_transactional_parallelized";
  }

  @BeforeClass
  public void beforeClass() throws Exception
  {
    doBeforeClass();
  }

  /**
   * This test can be run concurrently with other tests as it creates/modifies/teardowns a unique datasource
   * and supervisor maintained and scoped within this test only
   */
  @Test
  public void testKafkaIndexDataWithStartStopSupervisor() throws Exception
  {
    doTestIndexDataWithStartStopSupervisor(true);
  }

  /**
   * This test can be run concurrently with other tests as it creates/modifies/teardowns a unique datasource
   * and supervisor maintained and scoped within this test only
   */
  @Test
  public void testKafkaIndexDataWithKafkaReshardSplit() throws Exception
  {
    doTestIndexDataWithStreamReshardSplit(true);
  }
}
