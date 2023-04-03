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

package org.apache.druid.testsEx.indexer;

import org.apache.druid.testsEx.categories.TransactionalKafkaIndexSlow;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests Kafka ingestion while restarting nodes. Each test changes the state of
 * Druid cluster, but leaves the cluster in a stable state, ready for the next
 * test. As a result, the set of tests can be run as a single test using a
 * single Docker cluster.
 */
@RunWith(DruidTestRunner.class)
@Category(TransactionalKafkaIndexSlow.class)
public class ITKafkaIndexingServiceNonTransactionalSerializedTest extends AbstractKafkaIndexingServiceTest
{
  @Override
  public String getTestNamePrefix()
  {
    return "kafka_nontransactional_serialized";
  }

  @Before
  public void before() throws Exception
  {
    doBefore();
  }

  @Test
  public void testKafkaIndexDataWithLosingCoordinator() throws Exception
  {
    doTestIndexDataWithLosingCoordinator(false);
  }

  @Test
  public void testKafkaIndexDataWithLosingOverlord() throws Exception
  {
    doTestIndexDataWithLosingOverlord(false);
  }

  @Test
  public void testKafkaIndexDataWithLosingHistorical() throws Exception
  {
    doTestIndexDataWithLosingHistorical(false);
  }
}
