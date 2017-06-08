/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.overlord.supervisor;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.metadata.MetadataSupervisorManager;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the creation and lifetime of {@link Supervisor}.
 */
public class SupervisorManager
{
  private static final EmittingLogger log = new EmittingLogger(SupervisorManager.class);

  private final MetadataSupervisorManager metadataSupervisorManager;
  private final ConcurrentHashMap<String, Pair<Supervisor, SupervisorSpec>> supervisors = new ConcurrentHashMap<>();
  private final Object lock = new Object();

  private volatile boolean started = false;

  @Inject
  public SupervisorManager(MetadataSupervisorManager metadataSupervisorManager)
  {
    this.metadataSupervisorManager = metadataSupervisorManager;
  }

  public Set<String> getSupervisorIds()
  {
    return supervisors.keySet();
  }

  public Optional<SupervisorSpec> getSupervisorSpec(String id)
  {
    Pair<Supervisor, SupervisorSpec> supervisor = supervisors.get(id);
    return supervisor == null ? Optional.<SupervisorSpec>absent() : Optional.fromNullable(supervisor.rhs);
  }

  public boolean createOrUpdateAndStartSupervisor(SupervisorSpec spec)
  {
    Preconditions.checkState(started, "SupervisorManager not started");
    Preconditions.checkNotNull(spec, "spec");
    Preconditions.checkNotNull(spec.getId(), "spec.getId()");

    synchronized (lock) {
      Preconditions.checkState(started, "SupervisorManager not started");
      possiblyStopAndRemoveSupervisorInternal(spec.getId(), false);
      return createAndStartSupervisorInternal(spec, true);
    }
  }

  public boolean stopAndRemoveSupervisor(String id)
  {
    Preconditions.checkState(started, "SupervisorManager not started");
    Preconditions.checkNotNull(id, "id");

    synchronized (lock) {
      Preconditions.checkState(started, "SupervisorManager not started");
      return possiblyStopAndRemoveSupervisorInternal(id, true);
    }
  }

  @LifecycleStart
  public void start()
  {
    Preconditions.checkState(!started, "SupervisorManager already started");
    log.info("Loading stored supervisors from database");

    synchronized (lock) {
      Map<String, SupervisorSpec> supervisors = metadataSupervisorManager.getLatest();
      for (String id : supervisors.keySet()) {
        SupervisorSpec spec = supervisors.get(id);
        if (!(spec instanceof NoopSupervisorSpec)) {
          createAndStartSupervisorInternal(spec, false);
        }
      }

      started = true;
    }
  }

  @LifecycleStop
  public void stop()
  {
    Preconditions.checkState(started, "SupervisorManager not started");

    synchronized (lock) {
      for (String id : supervisors.keySet()) {
        try {
          supervisors.get(id).lhs.stop(false);
        }
        catch (Exception e) {
          log.warn(e, "Caught exception while stopping supervisor [%s]", id);
        }
      }
      supervisors.clear();
      started = false;
    }

    log.info("SupervisorManager stopped.");
  }

  public Map<String, List<VersionedSupervisorSpec>> getSupervisorHistory()
  {
    return metadataSupervisorManager.getAll();
  }

  public Optional<SupervisorReport> getSupervisorStatus(String id)
  {
    Pair<Supervisor, SupervisorSpec> supervisor = supervisors.get(id);
    return supervisor == null ? Optional.<SupervisorReport>absent() : Optional.fromNullable(supervisor.lhs.getStatus());
  }

  public boolean resetSupervisor(String id, @Nullable DataSourceMetadata dataSourceMetadata)
  {
    Preconditions.checkState(started, "SupervisorManager not started");
    Preconditions.checkNotNull(id, "id");

    Pair<Supervisor, SupervisorSpec> supervisor = supervisors.get(id);

    if (supervisor == null) {
      return false;
    }

    supervisor.lhs.reset(dataSourceMetadata);
    return true;
  }

  /**
   * Stops a supervisor with a given id and then removes it from the list.
   * <p/>
   * Caller should have acquired [lock] before invoking this method to avoid contention with other threads that may be
   * starting and stopping supervisors.
   *
   * @return true if a supervisor was stopped, false if there was no supervisor with this id
   */
  private boolean possiblyStopAndRemoveSupervisorInternal(String id, boolean writeTombstone)
  {
    Pair<Supervisor, SupervisorSpec> pair = supervisors.get(id);
    if (pair == null) {
      return false;
    }

    if (writeTombstone) {
      metadataSupervisorManager.insert(id, new NoopSupervisorSpec()); // where NoopSupervisorSpec is a tombstone
    }
    pair.lhs.stop(true);
    supervisors.remove(id);

    return true;
  }

  /**
   * Creates a supervisor from the provided spec and starts it if there is not already a supervisor with that id.
   * <p/>
   * Caller should have acquired [lock] before invoking this method to avoid contention with other threads that may be
   * starting and stopping supervisors.
   *
   * @return true if a new supervisor was created, false if there was already an existing supervisor with this id
   */
  private boolean createAndStartSupervisorInternal(SupervisorSpec spec, boolean persistSpec)
  {
    String id = spec.getId();
    if (supervisors.containsKey(id)) {
      return false;
    }

    if (persistSpec) {
      metadataSupervisorManager.insert(id, spec);
    }

    Supervisor supervisor = null;
    try {
      supervisor = spec.createSupervisor();
      supervisor.start();
    }
    catch (Exception e) {
      // Supervisor creation or start failed write tombstone only when trying to start a new supervisor
      if (persistSpec) {
        metadataSupervisorManager.insert(id, new NoopSupervisorSpec());
      }
      Throwables.propagate(e);
    }

    supervisors.put(id, Pair.of(supervisor, spec));
    return true;
  }
}
