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

package org.apache.druid.server.coordinator;

import org.joda.time.Duration;

public class TestDruidCoordinatorConfig extends DruidCoordinatorConfig
{
  private final Duration coordinatorStartDelay;
  private final Duration coordinatorPeriod;
  private final Duration coordinatorIndexingPeriod;
  private final Duration metadataStoreManagementPeriod;
  private final Duration loadTimeoutDelay;
  private final Duration coordinatorKillPeriod;
  private final Duration coordinatorKillDurationToRetain;
  private final Duration coordinatorSupervisorKillPeriod;
  private final Duration coordinatorSupervisorKillDurationToRetain;
  private final Duration coordinatorAuditKillPeriod;
  private final Duration coordinatorAuditKillDurationToRetain;
  private final Duration coordinatorCompactionKillPeriod;
  private final Duration coordinatorRuleKillPeriod;
  private final Duration coordinatorRuleKillDurationToRetain;
  private final Duration coordinatorDatasourceKillPeriod;
  private final Duration coordinatorDatasourceKillDurationToRetain;
  private final Duration loadQueuePeonRepeatDelay;
  private final int coordinatorKillMaxSegments;
  private final boolean compactionSkipLockedIntervals;
  private final boolean coordinatorKillIgnoreDurationToRetain;
  private final String loadQueuePeonType;
  private final Duration httpLoadQueuePeonRepeatDelay;
  private final int curatorLoadQueuePeonNumCallbackThreads;
  private final Duration httpLoadQueuePeonHostTimeout;
  private final int httpLoadQueuePeonBatchSize;

  public TestDruidCoordinatorConfig(
      Duration coordinatorStartDelay,
      Duration coordinatorPeriod,
      Duration coordinatorIndexingPeriod,
      Duration metadataStoreManagementPeriod,
      Duration loadTimeoutDelay,
      Duration coordinatorKillPeriod,
      Duration coordinatorKillDurationToRetain,
      Duration coordinatorSupervisorKillPeriod,
      Duration coordinatorSupervisorKillDurationToRetain,
      Duration coordinatorAuditKillPeriod,
      Duration coordinatorAuditKillDurationToRetain,
      Duration coordinatorCompactionKillPeriod,
      Duration coordinatorRuleKillPeriod,
      Duration coordinatorRuleKillDurationToRetain,
      Duration coordinatorDatasourceKillPeriod,
      Duration coordinatorDatasourceKillDurationToRetain,
      int coordinatorKillMaxSegments,
      Duration loadQueuePeonRepeatDelay,
      boolean compactionSkipLockedIntervals,
      boolean coordinatorKillIgnoreDurationToRetain,
      String loadQueuePeonType,
      Duration httpLoadQueuePeonRepeatDelay,
      Duration httpLoadQueuePeonHostTimeout,
      int httpLoadQueuePeonBatchSize,
      int curatorLoadQueuePeonNumCallbackThreads
  )
  {
    this.coordinatorStartDelay = coordinatorStartDelay;
    this.coordinatorPeriod = coordinatorPeriod;
    this.coordinatorIndexingPeriod = coordinatorIndexingPeriod;
    this.metadataStoreManagementPeriod = metadataStoreManagementPeriod;
    this.loadTimeoutDelay = loadTimeoutDelay;
    this.coordinatorKillPeriod = coordinatorKillPeriod;
    this.coordinatorKillDurationToRetain = coordinatorKillDurationToRetain;
    this.coordinatorSupervisorKillPeriod = coordinatorSupervisorKillPeriod;
    this.coordinatorSupervisorKillDurationToRetain = coordinatorSupervisorKillDurationToRetain;
    this.coordinatorAuditKillPeriod = coordinatorAuditKillPeriod;
    this.coordinatorAuditKillDurationToRetain = coordinatorAuditKillDurationToRetain;
    this.coordinatorCompactionKillPeriod = coordinatorCompactionKillPeriod;
    this.coordinatorRuleKillPeriod = coordinatorRuleKillPeriod;
    this.coordinatorRuleKillDurationToRetain = coordinatorRuleKillDurationToRetain;
    this.coordinatorDatasourceKillPeriod = coordinatorDatasourceKillPeriod;
    this.coordinatorDatasourceKillDurationToRetain = coordinatorDatasourceKillDurationToRetain;
    this.coordinatorKillMaxSegments = coordinatorKillMaxSegments;
    this.loadQueuePeonRepeatDelay = loadQueuePeonRepeatDelay;
    this.compactionSkipLockedIntervals = compactionSkipLockedIntervals;
    this.coordinatorKillIgnoreDurationToRetain = coordinatorKillIgnoreDurationToRetain;
    this.loadQueuePeonType = loadQueuePeonType;
    this.httpLoadQueuePeonRepeatDelay = httpLoadQueuePeonRepeatDelay;
    this.httpLoadQueuePeonHostTimeout = httpLoadQueuePeonHostTimeout;
    this.httpLoadQueuePeonBatchSize = httpLoadQueuePeonBatchSize;
    this.curatorLoadQueuePeonNumCallbackThreads = curatorLoadQueuePeonNumCallbackThreads;
  }

  @Override
  public Duration getCoordinatorStartDelay()
  {
    return coordinatorStartDelay;
  }

  @Override
  public Duration getCoordinatorPeriod()
  {
    return coordinatorPeriod;
  }

  @Override
  public Duration getCoordinatorIndexingPeriod()
  {
    return coordinatorIndexingPeriod;
  }

  @Override
  public Duration getCoordinatorMetadataStoreManagementPeriod()
  {
    return metadataStoreManagementPeriod;
  }

  @Override
  public Duration getCoordinatorKillPeriod()
  {
    return coordinatorKillPeriod;
  }

  @Override
  public Duration getCoordinatorKillDurationToRetain()
  {
    return coordinatorKillDurationToRetain;
  }

  @Override
  public Duration getCoordinatorSupervisorKillPeriod()
  {
    return coordinatorSupervisorKillPeriod;
  }

  @Override
  public Duration getCoordinatorSupervisorKillDurationToRetain()
  {
    return coordinatorSupervisorKillDurationToRetain;
  }

  @Override
  public Duration getCoordinatorAuditKillPeriod()
  {
    return coordinatorAuditKillPeriod;
  }

  @Override
  public Duration getCoordinatorAuditKillDurationToRetain()
  {
    return coordinatorAuditKillDurationToRetain;
  }

  @Override
  public Duration getCoordinatorCompactionKillPeriod()
  {
    return coordinatorCompactionKillPeriod;
  }

  @Override
  public Duration getCoordinatorRuleKillPeriod()
  {
    return coordinatorRuleKillPeriod;
  }

  @Override
  public Duration getCoordinatorRuleKillDurationToRetain()
  {
    return coordinatorRuleKillDurationToRetain;
  }

  @Override
  public Duration getCoordinatorDatasourceKillPeriod()
  {
    return coordinatorDatasourceKillPeriod;
  }

  @Override
  public Duration getCoordinatorDatasourceKillDurationToRetain()
  {
    return coordinatorDatasourceKillDurationToRetain;
  }

  @Override
  public int getCoordinatorKillMaxSegments()
  {
    return coordinatorKillMaxSegments;
  }

  @Override
  public Duration getLoadTimeoutDelay()
  {
    return loadTimeoutDelay == null ? super.getLoadTimeoutDelay() : loadTimeoutDelay;
  }

  @Override
  public Duration getLoadQueuePeonRepeatDelay()
  {
    return loadQueuePeonRepeatDelay;
  }

  @Override
  public boolean getCompactionSkipLockedIntervals()
  {
    return compactionSkipLockedIntervals;
  }

  @Override
  public boolean getCoordinatorKillIgnoreDurationToRetain()
  {
    return coordinatorKillIgnoreDurationToRetain;
  }

  @Override
  public String getLoadQueuePeonType()
  {
    return loadQueuePeonType;
  }

  @Override
  public Duration getHttpLoadQueuePeonRepeatDelay()
  {
    return httpLoadQueuePeonRepeatDelay;
  }

  @Override
  public int getNumCuratorCallBackThreads()
  {
    return curatorLoadQueuePeonNumCallbackThreads;
  }

  @Override
  public Duration getHttpLoadQueuePeonHostTimeout()
  {
    return httpLoadQueuePeonHostTimeout;
  }

  @Override
  public int getHttpLoadQueuePeonBatchSize()
  {
    return httpLoadQueuePeonBatchSize;
  }

  public static class Builder
  {
    private static final Duration DEFAULT_COORDINATOR_START_DELAY = new Duration("PT300s");
    private static final Duration DEFAULT_COORDINATOR_PERIOD = new Duration("PT60s");
    private static final Duration DEFAULT_COORDINATOR_INDEXING_PERIOD = new Duration("PT1800s");
    private static final Duration DEFAULT_METADATA_STORE_MANAGEMENT_PERIOD = new Duration("PT3600s");
    private static final Duration DEFAULT_COORDINATOR_KILL_PERIOD = new Duration("PT86400s");
    private static final Duration DEFAULT_COORDINATOR_KILL_DURATION_TO_RETAION = new Duration("PT7776000s");
    private static final boolean DEFAULT_COORDINATOR_KILL_IGNORE_DURATION_TO_RETAIN = false;
    private static final int DEFAULT_COORDINATOR_KILL_MAX_SEGMENTS = 100;
    private static final Duration DEFAULT_COORDINATOR_SUPERVISOR_KILL_PERIOD = new Duration("PT86400s");
    private static final Duration DEFAULT_COORDINATOR_SUPERVISOR_KILL_DURATION_TO_RETAIN = new Duration("PT7776000s");
    private static final Duration DEFAULT_COORDINATOR_COMPACTION_KILL_PERIOD = new Duration("PT86400s");
    private static final Duration DEFAULT_COORDINATOR_RULE_KILL_PERIOD = new Duration("PT86400s");
    private static final Duration DEFAULT_COORDINATOR_RULE_KILL_DURATION_TO_RETAIN = new Duration("PT7776000s");
    private static final Duration DEFAULT_COORDINATOR_DATASOURCE_KILL_PERIOD = new Duration("PT86400s");
    private static final Duration DEFAULT_COORDINATOR_DATASOURCE_KILL_DURATION_TO_RETAIN = new Duration("PT7776000s");
    private static final Duration DEFAULT_LOAD_TIMEOUT_DELAY = new Duration(15 * 60 * 1000);
    private static final Duration DEFAULT_LOAD_QUEUE_PEON_REPEAT_DELAY = Duration.millis(50);
    private static final String DEFAULT_LOAD_QUEUE_PEON_TYPE = "curator";
    private static final int DEFAULT_CURATOR_LOAD_QUEUE_PEON_NUM_CALLBACK_THREADS = 2;
    private static final Duration DEFAULT_HTTP_LOAD_QUEUE_PEON_REPEAT_DELAY = Duration.millis(60000);
    private static final Duration DEFAULT_HTTP_LOAD_QUEUE_PEON_HOST_TIMEOUT = Duration.millis(300000);
    private static final int DEFAULT_HTTP_LOAD_QUEUE_PEON_BATCH_SIZE = 1;
    private static final boolean DEFAULT_COMPACTION_SKIP_LOCKED_INTERVALS = true;
    private static final Duration DEFAULT_COORDINATOR_AUDIT_KILL_PERIOD = new Duration("PT86400s");
    private static final Duration DEFAULT_COORDINATOR_AUTIT_KILL_DURATION_TO_RETAIN = new Duration("PT7776000s");


    private Duration coordinatorStartDelay;
    private Duration coordinatorPeriod;
    private Duration coordinatorIndexingPeriod;
    private Duration metadataStoreManagementPeriod;
    private Duration coordinatorKillPeriod;
    private Duration coordinatorKillDurationToRetain;
    private Boolean coordinatorKillIgnoreDurationToRetain;
    private Integer coordinatorKillMaxSegments;
    private Duration coordinatorSupervisorKillPeriod;
    private Duration coordinatorSupervisorKillDurationToRetain;
    private Duration coordinatorCompactionKillPeriod;
    private Duration coordinatorRuleKillPeriod;
    private Duration coordinatorRuleKillDurationToRetain;
    private Duration coordinatorDatasourceKillPeriod;
    private Duration coordinatorDatasourceKillDurationToRetain;
    private Duration loadTimeoutDelay;
    private Duration loadQueuePeonRepeatDelay;
    private String loadQueuePeonType;
    private Duration httpLoadQueuePeonRepeatDelay;
    private Integer curatorLoadQueuePeonNumCallbackThreads;
    private Duration httpLoadQueuePeonHostTimeout;
    private Integer httpLoadQueuePeonBatchSize;
    private Boolean compactionSkippedLockedIntervals;
    private Duration coordinatorAuditKillPeriod;
    private Duration coordinatorAuditKillDurationToRetain;

    public Builder()
    {
    }

    public Builder withCoordinatorStartDelay(Duration coordinatorStartDelay)
    {
      this.coordinatorStartDelay = coordinatorStartDelay;
      return this;
    }

    public Builder withCoordinatorPeriod(Duration coordinatorPeriod)
    {
      this.coordinatorPeriod = coordinatorPeriod;
      return this;
    }

    public Builder withCoordinatorIndexingPeriod(Duration coordinatorIndexingPeriod)
    {
      this.coordinatorIndexingPeriod = coordinatorIndexingPeriod;
      return this;
    }

    public Builder withMetadataStoreManagementPeriod(Duration metadataStoreManagementPeriod)
    {
      this.metadataStoreManagementPeriod = metadataStoreManagementPeriod;
      return this;
    }

    public Builder withCoordinatorKillPeriod(Duration coordinatorKillPeriod)
    {
      this.coordinatorKillPeriod = coordinatorKillPeriod;
      return this;
    }

    public Builder withCoordinatorKillDurationToRetain(Duration coordinatorKillDurationToRetain)
    {
      this.coordinatorKillDurationToRetain = coordinatorKillDurationToRetain;
      return this;
    }

    public Builder withCoordinatorKillIgnoreDurationToRetain(boolean coordinatorKillIgnoreDurationToRetain)
    {
      this.coordinatorKillIgnoreDurationToRetain = coordinatorKillIgnoreDurationToRetain;
      return this;
    }

    public Builder withCoordinatorKillMaxSegments(int coordinatorKillMaxSegments)
    {
      this.coordinatorKillMaxSegments = coordinatorKillMaxSegments;
      return this;
    }

    public Builder withCoordinatorSupervisorKillPeriod(Duration coordinatorSupervisorKillPeriod)
    {
      this.coordinatorSupervisorKillPeriod = coordinatorSupervisorKillPeriod;
      return this;
    }

    public Builder withCoordinatorSupervisorKillDurationToRetain(Duration coordinatorSupervisorKillDurationToRetain)
    {
      this.coordinatorSupervisorKillDurationToRetain = coordinatorSupervisorKillDurationToRetain;
      return this;
    }

    public Builder withCoordinatorCompactionKillPeriod(Duration coordinatorCompactionKillPeriod)
    {
      this.coordinatorCompactionKillPeriod = coordinatorCompactionKillPeriod;
      return this;
    }

    public Builder withCoordinatorRuleKillPeriod(Duration coordinatorRuleKillPeriod)
    {
      this.coordinatorRuleKillPeriod = coordinatorRuleKillPeriod;
      return this;
    }

    public Builder withCoordinatorRuleKillDurationToRetain(Duration coordinatorRuleKillDurationToRetain)
    {
      this.coordinatorRuleKillDurationToRetain = coordinatorRuleKillDurationToRetain;
      return this;
    }

    public Builder withCoordinatorDatasourceKillPeriod(Duration coordinatorDatasourceKillPeriod)
    {
      this.coordinatorDatasourceKillPeriod = coordinatorDatasourceKillPeriod;
      return this;
    }

    public Builder withCoordinatorDatasourceKillDurationToRetain(Duration coordinatorDatasourceKillDurationToRetain)
    {
      this.coordinatorDatasourceKillDurationToRetain = coordinatorDatasourceKillDurationToRetain;
      return this;
    }

    public Builder withLoadTimeoutDelay(Duration loadTimeoutDelay)
    {
      this.loadTimeoutDelay = loadTimeoutDelay;
      return this;
    }

    public Builder withLoadQueuePeonRepeatDelay(Duration loadQueuePeonRepeatDelay)
    {
      this.loadQueuePeonRepeatDelay = loadQueuePeonRepeatDelay;
      return this;
    }

    public Builder withLoadQueuePeonType(String loadQueuePeonType)
    {
      this.loadQueuePeonType = loadQueuePeonType;
      return this;
    }

    public Builder withHttpLoadQueuePeonRepeatDelay(Duration httpLoadQueuePeonRepeatDelay)
    {
      this.httpLoadQueuePeonRepeatDelay = httpLoadQueuePeonRepeatDelay;
      return this;
    }

    public Builder withCuratorLoadQueuePeonNumCallbackThreads(int curatorLoadQueuePeonNumCallbackThreads)
    {
      this.curatorLoadQueuePeonNumCallbackThreads = curatorLoadQueuePeonNumCallbackThreads;
      return this;
    }

    public Builder withHttpLoadQueuePeonHostTimeout(Duration httpLoadQueuePeonHostTimeout)
    {
      this.httpLoadQueuePeonHostTimeout = httpLoadQueuePeonHostTimeout;
      return this;
    }

    public Builder withHttpLoadQueuePeonBatchSize(int httpLoadQueuePeonBatchSize)
    {
      this.httpLoadQueuePeonBatchSize = httpLoadQueuePeonBatchSize;
      return this;
    }

    public Builder withCompactionSkippedLockedIntervals(boolean compactionSkippedLockedIntervals)
    {
      this.compactionSkippedLockedIntervals = compactionSkippedLockedIntervals;
      return this;
    }

    public Builder withCoordianatorAuditKillPeriod(Duration coordinatorAuditKillPeriod)
    {
      this.coordinatorAuditKillPeriod = coordinatorAuditKillPeriod;
      return this;
    }

    public Builder withCoordinatorAuditKillDurationToRetain(Duration coordinatorAuditKillDurationToRetain)
    {
      this.coordinatorAuditKillDurationToRetain = coordinatorAuditKillDurationToRetain;
      return this;
    }

    public TestDruidCoordinatorConfig build()
    {
      return new TestDruidCoordinatorConfig(
          coordinatorStartDelay == null ? DEFAULT_COORDINATOR_START_DELAY : coordinatorStartDelay,
          coordinatorPeriod == null ? DEFAULT_COORDINATOR_PERIOD : coordinatorPeriod,
          coordinatorIndexingPeriod == null ? DEFAULT_COORDINATOR_INDEXING_PERIOD : coordinatorIndexingPeriod,
          metadataStoreManagementPeriod == null ? DEFAULT_METADATA_STORE_MANAGEMENT_PERIOD : metadataStoreManagementPeriod,
          loadTimeoutDelay == null ? DEFAULT_LOAD_TIMEOUT_DELAY : loadTimeoutDelay,
          coordinatorKillPeriod == null ? DEFAULT_COORDINATOR_KILL_PERIOD : coordinatorKillPeriod,
          coordinatorKillDurationToRetain == null ? DEFAULT_COORDINATOR_KILL_DURATION_TO_RETAION : coordinatorKillDurationToRetain,
          coordinatorSupervisorKillPeriod == null ? DEFAULT_COORDINATOR_SUPERVISOR_KILL_PERIOD : coordinatorSupervisorKillPeriod,
          coordinatorSupervisorKillDurationToRetain == null ? DEFAULT_COORDINATOR_SUPERVISOR_KILL_DURATION_TO_RETAIN : coordinatorSupervisorKillDurationToRetain,
          coordinatorAuditKillPeriod == null ? DEFAULT_COORDINATOR_AUDIT_KILL_PERIOD : coordinatorAuditKillPeriod,
          coordinatorAuditKillDurationToRetain == null ? DEFAULT_COORDINATOR_AUTIT_KILL_DURATION_TO_RETAIN : coordinatorAuditKillDurationToRetain,
          coordinatorCompactionKillPeriod == null ? DEFAULT_COORDINATOR_COMPACTION_KILL_PERIOD : coordinatorCompactionKillPeriod,
          coordinatorRuleKillPeriod == null ? DEFAULT_COORDINATOR_RULE_KILL_PERIOD : coordinatorRuleKillPeriod,
          coordinatorRuleKillDurationToRetain == null ? DEFAULT_COORDINATOR_RULE_KILL_DURATION_TO_RETAIN : coordinatorRuleKillDurationToRetain,
          coordinatorDatasourceKillPeriod == null ? DEFAULT_COORDINATOR_DATASOURCE_KILL_PERIOD : coordinatorDatasourceKillPeriod,
          coordinatorDatasourceKillDurationToRetain == null ? DEFAULT_COORDINATOR_DATASOURCE_KILL_DURATION_TO_RETAIN : coordinatorDatasourceKillDurationToRetain,
          coordinatorKillMaxSegments == null ? DEFAULT_COORDINATOR_KILL_MAX_SEGMENTS : coordinatorKillMaxSegments,
          loadQueuePeonRepeatDelay == null ? DEFAULT_LOAD_QUEUE_PEON_REPEAT_DELAY : loadQueuePeonRepeatDelay,
          compactionSkippedLockedIntervals == null ? DEFAULT_COMPACTION_SKIP_LOCKED_INTERVALS : compactionSkippedLockedIntervals,
          coordinatorKillIgnoreDurationToRetain == null ? DEFAULT_COORDINATOR_KILL_IGNORE_DURATION_TO_RETAIN : coordinatorKillIgnoreDurationToRetain,
          loadQueuePeonType == null ? DEFAULT_LOAD_QUEUE_PEON_TYPE : loadQueuePeonType,
          httpLoadQueuePeonRepeatDelay == null ? DEFAULT_HTTP_LOAD_QUEUE_PEON_REPEAT_DELAY : httpLoadQueuePeonRepeatDelay,
          httpLoadQueuePeonHostTimeout == null ? DEFAULT_HTTP_LOAD_QUEUE_PEON_HOST_TIMEOUT : httpLoadQueuePeonHostTimeout,
          httpLoadQueuePeonBatchSize == null ? DEFAULT_HTTP_LOAD_QUEUE_PEON_BATCH_SIZE : httpLoadQueuePeonBatchSize,
          curatorLoadQueuePeonNumCallbackThreads == null ? DEFAULT_CURATOR_LOAD_QUEUE_PEON_NUM_CALLBACK_THREADS
                                                         : curatorLoadQueuePeonNumCallbackThreads
      );
    }

  }
}
