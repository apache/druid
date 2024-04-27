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

package org.apache.druid.audit;


import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;

import java.io.IOException;
import java.util.List;

public interface AuditManager
{

  String X_DRUID_AUTHOR = "X-Druid-Author";
  String X_DRUID_COMMENT = "X-Druid-Comment";

  void doAudit(AuditEntry event);

  /**
   * Inserts an audit entry in audit table using the provided JDBI handle.
   * This method can be used to perform the audit in the same transaction as the
   * audited changes. Only SQL-based implementations need to implement this method,
   * other implementations call {@link #doAudit} by default.
   *
   * @param event  Event to audit
   * @param handle JDBI Handle representing connection to the database
   */
  default void doAudit(AuditEntry event, Handle handle) throws IOException
  {
    doAudit(event);
  }

  /**
   * Fetches audit entries made for the given key, type and interval. Implementations
   * that do not maintain an audit history should return an empty list.
   *
   * @return List of recorded audit events satisfying the passed parameters.
   */
  List<AuditEntry> fetchAuditHistory(String key, String type, Interval interval);

  /**
   * Fetches audit entries of a type whose audit time lies in the given interval.
   *
   * @param type     Type of audit entry
   * @param interval Eligible interval for audit time
   * @return List of recorded audit events satisfying the passed parameters.
   */
  List<AuditEntry> fetchAuditHistory(String type, Interval interval);

  /**
   * Provides last N entries of audit history for given key, type
   *
   * @return list of recorded audit events satisfying the passed parameters
   */
  List<AuditEntry> fetchAuditHistory(String key, String type, int limit);

  /**
   * Provides last N entries of audit history for given type
   *
   * @return List of recorded audit events satisfying the passed parameters.
   */
  List<AuditEntry> fetchAuditHistory(String type, int limit);

  /**
   * Remove audit logs created older than the given timestamp.
   *
   * @param timestamp timestamp in milliseconds
   * @return Number of audit logs removed
   */
  int removeAuditLogsOlderThan(long timestamp);
}
