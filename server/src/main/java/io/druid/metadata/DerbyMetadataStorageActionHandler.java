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

package io.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

import javax.annotation.Nullable;
import java.util.Map;

public class DerbyMetadataStorageActionHandler<EntryType, StatusType, LogType, LockType>
    extends SQLMetadataStorageActionHandler<EntryType, StatusType, LogType, LockType>
{
  @VisibleForTesting
  DerbyMetadataStorageActionHandler(
      SQLMetadataConnector connector,
      ObjectMapper jsonMapper,
      MetadataStorageActionHandlerTypes<EntryType, StatusType, LogType, LockType> types,
      String entryTypeName,
      String entryTable,
      String logTable,
      String lockTable
  )
  {
    super(connector, jsonMapper, types, entryTypeName, entryTable, logTable, lockTable);
  }

  @Override
  protected Query<Map<String, Object>> createInactiveStatusesSinceQuery(
      Handle handle, DateTime timestamp, @Nullable Integer maxNumStatuses
  )
  {
    String sql = StringUtils.format(
        "SELECT "
        + "  id, "
        + "  status_payload "
        + "FROM "
        + "  %s "
        + "WHERE "
        + "  active = FALSE AND created_date >= :start "
        + "ORDER BY created_date DESC",
        getEntryTable()
    );

    if (maxNumStatuses != null) {
      sql += " FETCH FIRST :n ROWS ONLY";
    }

    Query<Map<String, Object>> query = handle.createQuery(sql).bind("start", timestamp.toString());

    if (maxNumStatuses != null) {
      query = query.bind("n", maxNumStatuses);
    }
    return query;
  }
}
