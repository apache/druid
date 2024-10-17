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

package org.apache.druid.indexing.scheduledbatch;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.config.Configs;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.calcite.planner.ExplainAttributes;
import org.apache.druid.sql.client.BrokerClient;
import org.apache.druid.sql.http.ExplainPlanInformation;
import org.apache.druid.sql.http.SqlQuery;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class ScheduledBatchSupervisorSpec implements SupervisorSpec
{
  public static final String TYPE = "scheduled_batch";
  public static final String ID_PREFIX = "scheduled_batch__";

  private static final Logger log = new Logger(ScheduledBatchSupervisor.class);

  @JsonProperty
  private final SqlQuery spec;
  @JsonProperty
  private final boolean suspended;
  @JsonProperty
  private final CronSchedulerConfig schedulerConfig;

  /**
   * Note that both {@link #dataSource} and {@link #id} are optional JSON fields present in the spec.
   * They are only used internally because we use and persist the user-facing spec in the metadata store. So these
   * additional fields are required for jackson deserialization.
   * It would be better to have separate user-facing and domain-specific DTOs for this purpose and map them, but
   * that'll entail a larger change.
   */
  @JsonProperty
  private final String dataSource;
  @JsonProperty
  private final String id;

  private final ObjectMapper objectMapper;
  private final ScheduledBatchScheduler batchScheduler;
  private final BrokerClient brokerClient;

  @JsonCreator
  public ScheduledBatchSupervisorSpec(
      @JsonProperty("spec") final SqlQuery spec,
      @JsonProperty("schedulerConfig") final CronSchedulerConfig schedulerConfig,
      @JsonProperty("suspended") @Nullable Boolean suspended,
      @JsonProperty("id") @Nullable final String id,
      @JsonProperty("dataSource") @Nullable final String dataSource,
      @JacksonInject ObjectMapper objectMapper,
      @JacksonInject ScheduledBatchScheduler batchScheduler,
      @JacksonInject BrokerClient brokerClient
  )
  {
    this.spec = spec;
    this.schedulerConfig = schedulerConfig;
    this.suspended = Configs.valueOrDefault(suspended, false);
    this.objectMapper = objectMapper;
    this.batchScheduler = batchScheduler;
    this.brokerClient = brokerClient;

    this.dataSource = dataSource != null ? dataSource : getDatasourceFromQuery();
    this.id = id != null ? id : ID_PREFIX + this.dataSource + "__" + UUID.randomUUID();
  }

  private String getDatasourceFromQuery()
  {
    final List<ExplainPlanInformation> explainPlanInfos;
    final ListenableFuture<List<ExplainPlanInformation>> explainPlanFuture = brokerClient.explainPlanFor(spec);
    try {
      explainPlanInfos = explainPlanFuture.get();
    }
    catch (Exception e) {
      throw InvalidInput.exception("Error getting datasource from query[%s]: [%s]", spec, e);
    }

    if (explainPlanInfos.size() != 1) {
      throw DruidException.defensive(
          "Received an invalid EXPLAIN PLAN response for query[%s]. Expected a single plan information, but received[%d]: [%s].",
          spec.getQuery(), explainPlanInfos.size(), explainPlanInfos
      );
    }

    final ExplainAttributes explainAttributes = explainPlanInfos.get(0).getAttributes();

    if ("SELECT".equalsIgnoreCase(explainAttributes.getStatementType())) {
      throw InvalidInput.exception(
          "SELECT queries are not supported by the [%s] supervisor. "
          + "Only INSERT or REPLACE ingest queries are allowed.", getType());
    }

    return explainAttributes.getTargetDataSource();
  }

  @Override
  public String getId()
  {
    return id;
  }

  @Override
  public ScheduledBatchSupervisor createSupervisor()
  {
    return new ScheduledBatchSupervisor(this, batchScheduler);
  }

  @Override
  public List<String> getDataSources()
  {
    return Collections.singletonList(dataSource);
  }

  @Override
  public ScheduledBatchSupervisorSpec createSuspendedSpec()
  {
    return new ScheduledBatchSupervisorSpec(
        spec,
        schedulerConfig,
        true,
        id, dataSource,
        objectMapper,
        batchScheduler,
        null
    );
  }

  @Override
  public ScheduledBatchSupervisorSpec createRunningSpec()
  {
    return new ScheduledBatchSupervisorSpec(
        spec,
        schedulerConfig,
        false,
        id,
        dataSource,
        objectMapper,
        batchScheduler,
        null
    );
  }

  @Override
  public boolean isSuspended()
  {
    return suspended;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public String getSource()
  {
    return "";
  }

  public CronSchedulerConfig getSchedulerConfig()
  {
    return schedulerConfig;
  }

  public SqlQuery getSpec()
  {
    return spec;
  }
}
