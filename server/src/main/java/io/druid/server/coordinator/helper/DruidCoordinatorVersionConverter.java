/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.coordinator.helper;

import com.metamx.emitter.EmittingLogger;
import io.druid.client.indexing.IndexingServiceClient;
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

  public DruidCoordinatorVersionConverter(
      IndexingServiceClient indexingServiceClient,
      AtomicReference<DatasourceWhitelist> whitelistRef
  )
  {
    this.indexingServiceClient = indexingServiceClient;
    this.whitelistRef = whitelistRef;
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
