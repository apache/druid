/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
