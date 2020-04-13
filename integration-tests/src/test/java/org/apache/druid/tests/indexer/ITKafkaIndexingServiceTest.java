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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

@Test(groups = TestNGGroup.KAFKA_INDEX)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITKafkaIndexingServiceTest extends AbstractKafkaIndexerTest
{
  private static final Logger LOG = new Logger(ITKafkaIndexingServiceTest.class);
  private static final String DATASOURCE = "kafka_indexing_service_test";

  @DataProvider
  public static Object[][] testParams()
  {
    return new Object[][]{
        {"legacy_parser"},
        {"input_format"}
    };
  }

  @Test(dataProvider = "testParams")
  public void testKafka(String param)
  {
    final String supervisorSpecPath = "legacy_parser".equals(param)
                                      ? INDEXER_FILE_LEGACY_PARSER
                                      : INDEXER_FILE_INPUT_FORMAT;
    LOG.info("Starting test: ITKafkaIndexingServiceTest");
    doKafkaIndexTest(StringUtils.format("%s_%s", DATASOURCE, param), supervisorSpecPath, false);
  }

  @AfterMethod
  public void afterClass()
  {
    LOG.info("teardown");
    doTearDown();
  }
}
