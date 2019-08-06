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

package org.apache.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.segment.TestHelper;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.stream.Collectors;


/**
 * Like {@link SQLMetadataRuleManagerTest} except with no segments to make sure it behaves when it's empty
 */
public class SQLMetadataSegmentManagerEmptyTest
{

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private SQLMetadataSegmentManager sqlSegmentsMetadata;
  private final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

  @Before
  public void setUp() throws Exception
  {
    TestDerbyConnector connector = derbyConnectorRule.getConnector();
    MetadataSegmentManagerConfig config = new MetadataSegmentManagerConfig();
    config.setPollDuration(Period.seconds(1));
    sqlSegmentsMetadata = new SQLMetadataSegmentManager(
        jsonMapper,
        Suppliers.ofInstance(config),
        derbyConnectorRule.metadataTablesConfigSupplier(),
        connector
    );
    sqlSegmentsMetadata.start();

    connector.createSegmentTable();
  }

  @After
  public void teardown()
  {
    if (sqlSegmentsMetadata.isPollingDatabasePeriodically()) {
      sqlSegmentsMetadata.stopPollingDatabasePeriodically();
    }
    sqlSegmentsMetadata.stop();
  }

  @Test
  public void testPollEmpty()
  {
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.poll();
    Assert.assertTrue(sqlSegmentsMetadata.isPollingDatabasePeriodically());
    Assert.assertEquals(
        ImmutableList.of(),
        sqlSegmentsMetadata.retrieveAllDataSourceNames()
    );
    Assert.assertEquals(
        ImmutableList.of(),
        sqlSegmentsMetadata
            .getImmutableDataSourcesWithAllUsedSegments()
            .stream()
            .map(ImmutableDruidDataSource::getName)
            .collect(Collectors.toList())
    );
    Assert.assertEquals(
        null,
        sqlSegmentsMetadata.getImmutableDataSourceWithUsedSegments("wikipedia")
    );
    Assert.assertEquals(
        ImmutableSet.of(),
        ImmutableSet.copyOf(sqlSegmentsMetadata.iterateAllUsedSegments())
    );
  }

  @Test
  public void testStopAndStart()
  {
    // Simulate successive losing and getting the coordinator leadership
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.stopPollingDatabasePeriodically();
    sqlSegmentsMetadata.startPollingDatabasePeriodically();
    sqlSegmentsMetadata.stopPollingDatabasePeriodically();
  }
}
