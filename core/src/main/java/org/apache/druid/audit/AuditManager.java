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

  /**
   * inserts an audit Entry in the Audit Table
   * @param auditEntry
   */
  void doAudit(AuditEntry auditEntry);

  /**
   * inserts an audit Entry in audit table using the handler provided
   * used to do the audit in same transaction as the config changes
   * @param auditEntry
   * @param handler
   * @throws IOException
   */
  void doAudit(AuditEntry auditEntry, Handle handler) throws IOException;

  /**
   * provides audit history for given key, type and interval
   * @param key
   * @param type
   * @param interval
   * @return list of AuditEntries satisfying the passed parameters
   */
  List<AuditEntry> fetchAuditHistory(String key, String type, Interval interval);

  /**
   * provides audit history for given type and interval
   * @param type type of auditEntry
   * @param interval interval for which to fetch auditHistory
   * @return list of AuditEntries satisfying the passed parameters
   */
  List<AuditEntry> fetchAuditHistory(String type, Interval interval);

  /**
   * Provides last N entries of audit history for given key, type
   * @param key
   * @param type
   * @param limit
   * @return list of AuditEntries satisfying the passed parameters
   */
  List<AuditEntry> fetchAuditHistory(String key, String type, int limit);

  /**
   * Provides last N entries of audit history for given type
   * @param type type of auditEntry
   * @param limit
   * @return list of AuditEntries satisfying the passed parameters
   */
  List<AuditEntry> fetchAuditHistory(String type, int limit);
}
