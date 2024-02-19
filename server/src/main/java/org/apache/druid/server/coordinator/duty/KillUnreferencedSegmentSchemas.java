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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.metadata.SchemaManager;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;

import javax.annotation.Nullable;

/**
 *
 */
public class KillUnreferencedSegmentSchemas implements CoordinatorDuty
{
  private static final Logger log = new Logger(KillUnreferencedSegmentSchemas.class);
  private final SchemaManager schemaManager;

  public KillUnreferencedSegmentSchemas(SchemaManager schemaManager)
  {
    this.schemaManager = schemaManager;
  }

  @Nullable
  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    int deleted = schemaManager.cleanUpUnreferencedSchema();
    log.info("Cleaned up [%d] unreferenced schemas from the DB.", deleted);
    // todo should retrigger schema poll from db?
    return params;
  }
}
