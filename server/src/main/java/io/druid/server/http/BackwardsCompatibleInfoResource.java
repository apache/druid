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

package io.druid.server.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.druid.client.InventoryView;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.db.DatabaseRuleManager;
import io.druid.db.DatabaseSegmentManager;
import io.druid.server.coordinator.DruidCoordinator;

import javax.annotation.Nullable;
import javax.ws.rs.Path;

/**
 */
@Deprecated
@Path("/static/info")
public class BackwardsCompatibleInfoResource extends InfoResource
{
  @Inject
  public BackwardsCompatibleInfoResource(
      DruidCoordinator coordinator,
      InventoryView serverInventoryView,
      DatabaseSegmentManager databaseSegmentManager,
      DatabaseRuleManager databaseRuleManager,
      @Nullable IndexingServiceClient indexingServiceClient,
      ObjectMapper jsonMapper
  )
  {
    super(
        coordinator,
        serverInventoryView,
        databaseSegmentManager,
        databaseRuleManager,
        indexingServiceClient,
        jsonMapper
    );
  }
}
