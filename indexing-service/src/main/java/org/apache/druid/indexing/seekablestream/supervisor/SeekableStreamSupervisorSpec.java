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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.annotations.SuppressFBWarnings;
import org.apache.druid.common.config.Configs;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoScaler;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClientFactory;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.NoopTaskAutoScaler;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class SeekableStreamSupervisorSpec implements SupervisorSpec
{

  protected static final String ILLEGAL_INPUT_SOURCE_UPDATE_ERROR_MESSAGE =
      "Update of the input source stream from [%s] to [%s] is not supported for a running supervisor."
      + "%nTo perform the update safely, follow these steps:"
      + "%n(1) Suspend this supervisor, reset its offsets and then terminate it. "
      + "%n(2) Create a new supervisor with the new input source stream."
      + "%nNote that doing the reset can cause data duplication or loss if any topic used in the old supervisor is included in the new one too.";

  private static SeekableStreamSupervisorIngestionSpec checkIngestionSchema(
      SeekableStreamSupervisorIngestionSpec ingestionSchema
  )
  {
    Preconditions.checkNotNull(ingestionSchema, "ingestionSchema");
    Preconditions.checkNotNull(ingestionSchema.getDataSchema(), "dataSchema");
    Preconditions.checkNotNull(ingestionSchema.getIOConfig(), "ioConfig");
    return ingestionSchema;
  }

  protected final String id;
  protected final TaskStorage taskStorage;
  protected final TaskMaster taskMaster;
  protected final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  protected final SeekableStreamIndexTaskClientFactory indexTaskClientFactory;
  protected final ObjectMapper mapper;
  protected final RowIngestionMetersFactory rowIngestionMetersFactory;
  private final SeekableStreamSupervisorIngestionSpec ingestionSchema;
  @Nullable
  private final Map<String, Object> context;
  protected final ServiceEmitter emitter;
  protected final DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private final boolean suspended;
  protected final SupervisorStateManagerConfig supervisorStateManagerConfig;

  /**
   * Base constructor for SeekableStreamSupervisors.
   * The unique identifier for the supervisor. A null {@code id} implies the constructor will use the
   * non-null `dataSource` in `ingestionSchema` for backwards compatibility.
   */
  public SeekableStreamSupervisorSpec(
      @Nullable final String id,
      final SeekableStreamSupervisorIngestionSpec ingestionSchema,
      @Nullable Map<String, Object> context,
      Boolean suspended,
      TaskStorage taskStorage,
      TaskMaster taskMaster,
      IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      SeekableStreamIndexTaskClientFactory indexTaskClientFactory,
      @Json ObjectMapper mapper,
      ServiceEmitter emitter,
      DruidMonitorSchedulerConfig monitorSchedulerConfig,
      RowIngestionMetersFactory rowIngestionMetersFactory,
      SupervisorStateManagerConfig supervisorStateManagerConfig
  )
  {
    this.ingestionSchema = checkIngestionSchema(ingestionSchema);
    this.id = Preconditions.checkNotNull(
        Configs.valueOrDefault(id, ingestionSchema.getDataSchema().getDataSource()),
        "spec id cannot be null!"
    );
    this.context = context;

    this.taskStorage = taskStorage;
    this.taskMaster = taskMaster;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.indexTaskClientFactory = indexTaskClientFactory;
    this.mapper = mapper;
    this.emitter = emitter;
    this.monitorSchedulerConfig = monitorSchedulerConfig;
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;
    this.suspended = suspended != null ? suspended : false;
    this.supervisorStateManagerConfig = supervisorStateManagerConfig;
  }

  @JsonProperty
  public SeekableStreamSupervisorIngestionSpec getSpec()
  {
    return ingestionSchema;
  }

  @Deprecated
  @JsonProperty
  public DataSchema getDataSchema()
  {
    return ingestionSchema.getDataSchema();
  }

  @JsonProperty
  public SeekableStreamSupervisorTuningConfig getTuningConfig()
  {
    return ingestionSchema.getTuningConfig();
  }

  @JsonProperty
  public SeekableStreamSupervisorIOConfig getIoConfig()
  {
    return ingestionSchema.getIOConfig();
  }

  @Nullable
  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
  }

  @Nullable
  public <ContextValueType> ContextValueType getContextValue(String key)
  {
    return context == null ? null : (ContextValueType) context.get(key);
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  /**
   * Returns the identifier for this supervisor.
   * If unspecified, defaults to the dataSource being written to.
   */
  @Override
  @JsonProperty
  public String getId()
  {
    return id;
  }

  public DruidMonitorSchedulerConfig getMonitorSchedulerConfig()
  {
    return monitorSchedulerConfig;
  }

  @Override
  public abstract Supervisor createSupervisor();

  /**
   * An autoScaler instance will be returned depending on the autoScalerConfig. In case autoScalerConfig is null or autoScaler is disabled then NoopTaskAutoScaler will be returned.
   *
   * @param supervisor
   * @return autoScaler
   */
  @Override
  public SupervisorTaskAutoScaler createAutoscaler(Supervisor supervisor)
  {
    AutoScalerConfig autoScalerConfig = ingestionSchema.getIOConfig().getAutoScalerConfig();
    if (autoScalerConfig != null && autoScalerConfig.getEnableTaskAutoScaler() && supervisor instanceof SeekableStreamSupervisor) {
      return autoScalerConfig.createAutoScaler(supervisor, this, emitter);
    }
    return new NoopTaskAutoScaler();
  }

  @Override
  public List<String> getDataSources()
  {
    return ImmutableList.of(getDataSchema().getDataSource());
  }

  @Override
  public SeekableStreamSupervisorSpec createSuspendedSpec()
  {
    return toggleSuspend(true);
  }

  @Override
  public SeekableStreamSupervisorSpec createRunningSpec()
  {
    return toggleSuspend(false);
  }

  public SupervisorStateManagerConfig getSupervisorStateManagerConfig()
  {
    return supervisorStateManagerConfig;
  }

  @Override
  @JsonProperty("suspended")
  public boolean isSuspended()
  {
    return suspended;
  }

  /**
   * Default implementation that prevents unsupported evolution of the supervisor spec
   * <ul>
   * <li>You cannot migrate between types of supervisors.</li>
   * <li>You cannot change the input source stream of a running supervisor.</li>
   * </ul>
   *
   * @param proposedSpec the proposed supervisor spec
   * @throws DruidException if the proposed spec update is not allowed
   */
  @Override
  public void validateSpecUpdateTo(SupervisorSpec proposedSpec) throws DruidException
  {
    if (!(proposedSpec instanceof SeekableStreamSupervisorSpec)) {
      throw InvalidInput.exception(
          "Cannot update supervisor spec from type[%s] to type[%s]",
          getClass().getSimpleName(),
          proposedSpec.getClass().getSimpleName()
      );
    }
    SeekableStreamSupervisorSpec other = (SeekableStreamSupervisorSpec) proposedSpec;
    if (this.getSource() == null || other.getSource() == null) {
      // Not likely to happen, but covering just in case.
      throw InvalidInput.exception("Cannot update supervisor spec since one or both of "
                                   + "the specs have not provided an input source stream in the 'ioConfig'.");
    }

    if (!this.getSource().equals(other.getSource())) {
      throw InvalidInput.exception(ILLEGAL_INPUT_SOURCE_UPDATE_ERROR_MESSAGE, this.getSource(), other.getSource());
    }
  }

  /**
   * Updates {@link SeekableStreamSupervisorIOConfig#getTaskCount()} on this user-submitted spec
   * to the desired value. The rules applied are:
   *
   * <ol>
   *   <li>If {@code taskCountStart} is set on this user-submitted spec, use it.</li>
   *   <li>Otherwise, if {@code taskCount} is set on this user-submitted spec, use it.</li>
   *   <li>Otherwise, use the existing spec's {@code taskCount}.</li>
   * </ol>
   */
  @Override
  public void merge(@Nullable SupervisorSpec existingSpec)
  {
    // Use this spec's taskCountStart if set.
    final AutoScalerConfig thisAutoScalerConfig = getIoConfig().getAutoScalerConfig();
    if (thisAutoScalerConfig != null
        && thisAutoScalerConfig.getEnableTaskAutoScaler()
        && thisAutoScalerConfig.getTaskCountStart() != null) {
      getIoConfig().setTaskCount(thisAutoScalerConfig.getTaskCountStart());
      return;
    }

    // Use this spec's taskCount if set.
    if (getIoConfig().isTaskCountExplicit()) {
      return;
    }

    // Use the existing spec's taskCount. If it isn't there, we'll fall back to this spec's taskCount. Because there's
    // no taskCountStart (and taskCount hasn't been explicitly set) this spec's taskCount will be taskCountMin or 1.
    if (existingSpec instanceof SeekableStreamSupervisorSpec existingSeekableStreamSpec) {
      getIoConfig().setTaskCount(existingSeekableStreamSpec.getIoConfig().getTaskCount());
    }
  }

  /**
   * Common restart decision shared by all seekable-stream supervisors. A copy of the proposed spec
   * ({@code this}) is taken, differences that do not require a restart ("ignore cases") are
   * neutralized on that copy, and the result is compared to the running {@code old} spec for
   * equality. Returns {@code true} when a restart is needed.
   * <p>
   * Ignore cases work on a copy so the proposed spec — which may be persisted — is never mutated:
   * <ul>
   *   <li>{@code ioConfig.taskCount} when autoscaling is enabled on either spec, since it is
   *       overridden at runtime.</li>
   * </ul>
   * Everything else (dataSchema, tuningConfig, the rest of ioConfig, context, suspended) must match,
   * as determined by {@link #equals(Object)}. Subclasses may add type-specific rules by calling
   * {@code super.requireRestart(old)} first.
   */
  @Override
  public boolean requireRestart(SupervisorSpec old)
  {
    if (!(old instanceof SeekableStreamSupervisorSpec other)) {
      return true;
    }

    // Start from a builder initialized with this (proposed) spec, neutralize the ignore-case fields
    // via builder setters, then build and compare. The builder produces a copy, so the proposed spec
    // — which may be persisted — is never mutated.
    final Builder<?> proposed;
    try {
      proposed = toBuilder();
    }
    catch (final UnsupportedOperationException e) {
      return true;
    }

    // Ignore case: taskCount is overridden at runtime when autoscaling is enabled, so align it to
    // the existing spec's value.
    if (isAutoScalerEnabled() || other.isAutoScalerEnabled()) {
      proposed.taskCount(other.getIoConfig().getTaskCount());
    }

    return !proposed.build().equals(other);
  }

  private boolean isAutoScalerEnabled()
  {
    final AutoScalerConfig autoScalerConfig = getIoConfig().getAutoScalerConfig();
    return autoScalerConfig != null && autoScalerConfig.getEnableTaskAutoScaler();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SeekableStreamSupervisorSpec that = (SeekableStreamSupervisorSpec) o;
    // Injected services (taskStorage, mapper, emitter, etc.) are excluded; only the user-defined
    // spec content determines equality.
    return suspended == that.suspended
           && Objects.equals(id, that.id)
           && Objects.equals(ingestionSchema, that.ingestionSchema)
           && Objects.equals(context, that.context);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(id, ingestionSchema, context, suspended);
  }

  protected abstract SeekableStreamSupervisorSpec toggleSuspend(boolean suspend);

  public abstract SeekableStreamSupervisorSpec createBackfillSpec(
      String backfillId,
      BoundedStreamConfig boundedStreamConfig,
      @Nullable Integer taskCount
  );

  /**
   * Returns a builder pre-populated with this spec's values (including injected services), so callers
   * can produce a modified copy without mutating this instance. Subclasses return their own builder.
   */
  public Builder<?> toBuilder()
  {
    throw new UnsupportedOperationException("No builder is available for this supervisor spec.");
  }

  /**
   * Self-typed builder for {@link SeekableStreamSupervisorSpec} and its subclasses. Holds the spec's
   * components and injected services; subclasses implement {@link #self()} and {@link #build()} to
   * reconstruct the concrete spec. Setter methods cover fields commonly copied or adjusted by callers.
   */
  @SuppressFBWarnings(
      value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD",
      justification = "Fields are populated via copyFrom() and read by build() in concrete subclasses, which "
                      + "live in other modules and so are invisible to SpotBugs' per-module analysis."
  )
  public abstract static class Builder<T extends Builder<T>>
  {
    protected String id;
    protected DataSchema dataSchema;
    protected SeekableStreamSupervisorIOConfig ioConfig;
    protected SeekableStreamSupervisorTuningConfig tuningConfig;
    protected Map<String, Object> context;
    protected Boolean suspended;
    protected TaskStorage taskStorage;
    protected TaskMaster taskMaster;
    protected IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
    protected SeekableStreamIndexTaskClientFactory indexTaskClientFactory;
    protected ObjectMapper mapper;
    protected ServiceEmitter emitter;
    protected DruidMonitorSchedulerConfig monitorSchedulerConfig;
    protected RowIngestionMetersFactory rowIngestionMetersFactory;
    protected SupervisorStateManagerConfig supervisorStateManagerConfig;

    protected abstract T self();

    public abstract SeekableStreamSupervisorSpec build();

    /**
     * Copies all fields (components and injected services) from an existing spec into this builder.
     */
    public T copyFrom(SeekableStreamSupervisorSpec spec)
    {
      this.id = spec.id;
      this.dataSchema = spec.getSpec().getDataSchema();
      this.ioConfig = spec.getIoConfig();
      this.tuningConfig = spec.getTuningConfig();
      this.context = spec.context;
      this.suspended = spec.suspended;
      this.taskStorage = spec.taskStorage;
      this.taskMaster = spec.taskMaster;
      this.indexerMetadataStorageCoordinator = spec.indexerMetadataStorageCoordinator;
      this.indexTaskClientFactory = spec.indexTaskClientFactory;
      this.mapper = spec.mapper;
      this.emitter = spec.emitter;
      this.monitorSchedulerConfig = spec.monitorSchedulerConfig;
      this.rowIngestionMetersFactory = spec.rowIngestionMetersFactory;
      this.supervisorStateManagerConfig = spec.supervisorStateManagerConfig;
      return self();
    }

    public T id(String id)
    {
      this.id = id;
      return self();
    }

    public T dataSchema(DataSchema dataSchema)
    {
      this.dataSchema = dataSchema;
      return self();
    }

    public T ioConfig(SeekableStreamSupervisorIOConfig ioConfig)
    {
      this.ioConfig = ioConfig;
      return self();
    }

    public T tuningConfig(SeekableStreamSupervisorTuningConfig tuningConfig)
    {
      this.tuningConfig = tuningConfig;
      return self();
    }

    public T context(Map<String, Object> context)
    {
      this.context = context;
      return self();
    }

    public T suspended(boolean suspended)
    {
      this.suspended = suspended;
      return self();
    }

    /**
     * Sets {@code ioConfig.taskCount} on a copy of the current ioConfig (never mutates the original).
     */
    public T taskCount(int taskCount)
    {
      this.ioConfig = this.ioConfig.toBuilder().withTaskCount(taskCount).build();
      return self();
    }
  }

}
