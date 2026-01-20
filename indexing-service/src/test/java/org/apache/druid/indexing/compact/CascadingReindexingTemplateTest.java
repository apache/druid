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

package org.apache.druid.indexing.compact;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.guice.SupervisorModule;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.compaction.InlineReindexingRuleProvider;
import org.apache.druid.server.compaction.ReindexingGranularityRule;
import org.apache.druid.server.compaction.ReindexingRuleProvider;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class CascadingReindexingTemplateTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    OBJECT_MAPPER.registerModules(new SupervisorModule().getJacksonModules());
  }

  @Test
  public void test_serde() throws Exception
  {
    final CascadingReindexingTemplate template = new CascadingReindexingTemplate(
        "testDataSource",
        50,
        1000000L,
        InlineReindexingRuleProvider.builder()
            .granularityRules(List.of(
                new ReindexingGranularityRule(
                    "hourRule",
                    null,
                    Period.days(7),
                    new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null)
                ),
                new ReindexingGranularityRule(
                    "dayRule",
                    null,
                    Period.days(30),
                    new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null)
                )
            ))
            .build(),
        CompactionEngine.NATIVE,
        ImmutableMap.of("context_key", "context_value")
    );

    final String json = OBJECT_MAPPER.writeValueAsString(template);
    final CascadingReindexingTemplate fromJson = OBJECT_MAPPER.readValue(json, CascadingReindexingTemplate.class);

    Assert.assertEquals(template.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(template.getTaskPriority(), fromJson.getTaskPriority());
    Assert.assertEquals(template.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(template.getEngine(), fromJson.getEngine());
    Assert.assertEquals(template.getTaskContext(), fromJson.getTaskContext());
    Assert.assertEquals(template.getType(), fromJson.getType());
  }

  @Test
  public void test_serde_asDataSourceCompactionConfig() throws Exception
  {
    final CascadingReindexingTemplate template = new CascadingReindexingTemplate(
        "testDataSource",
        30,
        500000L,
        InlineReindexingRuleProvider.builder()
            .granularityRules(List.of(
                new ReindexingGranularityRule(
                    "rule1",
                    null,
                    Period.days(7),
                    new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null)
                )
            ))
            .build(),
        CompactionEngine.MSQ,
        ImmutableMap.of("key", "value")
    );

    // Serialize and deserialize as DataSourceCompactionConfig interface
    final String json = OBJECT_MAPPER.writeValueAsString(template);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertTrue(fromJson instanceof CascadingReindexingTemplate);
    final CascadingReindexingTemplate cascadingFromJson = (CascadingReindexingTemplate) fromJson;

    Assert.assertEquals("testDataSource", cascadingFromJson.getDataSource());
    Assert.assertEquals(30, cascadingFromJson.getTaskPriority());
    Assert.assertEquals(500000L, cascadingFromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(CompactionEngine.MSQ, cascadingFromJson.getEngine());
    Assert.assertEquals(ImmutableMap.of("key", "value"), cascadingFromJson.getTaskContext());
    Assert.assertEquals(CascadingReindexingTemplate.TYPE, cascadingFromJson.getType());
  }

  @Test
  public void test_createCompactionJobs_ruleProviderNotReady()
  {
    final ReindexingRuleProvider notReadyProvider = EasyMock.createMock(ReindexingRuleProvider.class);
    EasyMock.expect(notReadyProvider.isReady()).andReturn(false);
    EasyMock.expect(notReadyProvider.getType()).andReturn("mock-provider");
    EasyMock.replay(notReadyProvider);

    final CascadingReindexingTemplate template = new CascadingReindexingTemplate(
        "testDataSource",
        null,
        null,
        notReadyProvider,
        null,
        null
    );

    // Call createCompactionJobs - should return empty list without processing
    final List<CompactionJob> jobs = template.createCompactionJobs(null, null);

    Assert.assertTrue(jobs.isEmpty());
    EasyMock.verify(notReadyProvider);
  }
}
