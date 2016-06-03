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
import com.google.inject.Inject;
import com.metamx.common.Pair;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import io.druid.metadata.MetadataSupervisorManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Manages the creation and lifetime of {@link Supervisor}.
 */
public class SupervisorManager
{
  private static final EmittingLogger log = new EmittingLogger(SupervisorManager.class);

  private final MetadataSupervisorManager metadataSupervisorManager;
  private final Map<String, Pair<Supervisor, SupervisorSpec>> supervisors = new HashMap<>();

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
    return supervisors.get(id) == null
           ? Optional.<SupervisorSpec>absent()
           : Optional.fromNullable(supervisors.get(id).rhs);
  }

  public boolean hasSupervisor(String id)
  {
    return supervisors.containsKey(id);
  }

  public boolean createAndStartSupervisor(SupervisorSpec spec)
  {
    Preconditions.checkNotNull(spec, "spec");
    Preconditions.checkNotNull(spec.getId(), "spec.getId()");

    return createAndStartSupervisorInternal(spec, true);
  }

  public void stopAndRemoveSupervisor(String id, boolean writeTombstone)
  {
    Pair<Supervisor, SupervisorSpec> pair = supervisors.get(id);
    if (pair != null) {
      if (writeTombstone) {
        metadataSupervisorManager.insert(id, new NoopSupervisorSpec()); // where NoopSupervisorSpec is a tombstone
      }
      pair.lhs.stop(true);
      supervisors.remove(id);
    }
  }

  @LifecycleStart
  public void start()
  {
    log.info("Loading stored supervisors from database");

    Map<String, SupervisorSpec> supervisors = metadataSupervisorManager.getLatest();
    for (String id : supervisors.keySet()) {
      SupervisorSpec spec = supervisors.get(id);
      if (!(spec instanceof NoopSupervisorSpec)) {
        createAndStartSupervisorInternal(spec, false);
      }
    }
  }

  @LifecycleStop
  public void stop()
  {
    for (String id : supervisors.keySet()) {
      supervisors.get(id).lhs.stop(false);
    }

    supervisors.clear();
    log.info("SupervisorManager stopped.");
  }

  public Map<String, List<VersionedSupervisorSpec>> getSupervisorHistory()
  {
    return metadataSupervisorManager.getAll();
  }

  public Optional<SupervisorReport> getSupervisorStatus(String id)
  {
    return supervisors.get(id) == null
           ? Optional.<SupervisorReport>absent()
           : Optional.fromNullable(supervisors.get(id).lhs.getStatus());
  }

  private boolean createAndStartSupervisorInternal(SupervisorSpec spec, boolean persistSpec)
  {
    String id = spec.getId();
    if (!supervisors.containsKey(id)) {
      Supervisor supervisor = spec.createSupervisor();
      supervisor.start(); // try starting the supervisor first so we don't persist a bad spec

      if (persistSpec) {
        metadataSupervisorManager.insert(id, spec);
      }

      supervisors.put(id, Pair.of(supervisor, spec));
      return true;
    }

    return false;
  }
}
