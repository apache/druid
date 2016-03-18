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

package io.druid.server.coordinator.helper;

import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.common.config.JacksonConfigManager;
import io.druid.segment.IndexIO;
import io.druid.server.coordinator.DatasourceWhitelist;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.timeline.DataSegment;

import java.util.concurrent.atomic.AtomicReference;

public class DruidCoordinatorVersionConverter implements DruidCoordinatorHelper
{
  private static final EmittingLogger log = new EmittingLogger(DruidCoordinatorVersionConverter.class);

  private final IndexingServiceClient indexingServiceClient;
  private final AtomicReference<DatasourceWhitelist> whitelistRef;

  @Inject
  public DruidCoordinatorVersionConverter(
      IndexingServiceClient indexingServiceClient,
      JacksonConfigManager configManager
  )
  {
    this.indexingServiceClient = indexingServiceClient;
    this.whitelistRef = configManager.watch(DatasourceWhitelist.CONFIG_KEY, DatasourceWhitelist.class);
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    DatasourceWhitelist whitelist = whitelistRef.get();

    for (DataSegment dataSegment : params.getAvailableSegments()) {
      if (whitelist == null || whitelist.contains(dataSegment.getDataSource())) {
        final Integer binaryVersion = dataSegment.getBinaryVersion();

        if (binaryVersion == null || binaryVersion < IndexIO.CURRENT_VERSION_ID) {
          log.info("Upgrading version on segment[%s]", dataSegment.getIdentifier());
          indexingServiceClient.upgradeSegment(dataSegment);
        }
      }
    }

    return params;
  }
}
