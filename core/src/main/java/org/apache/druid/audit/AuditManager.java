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


import org.apache.druid.common.config.ConfigSerde;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;

import java.io.IOException;
import java.util.List;

public interface AuditManager
{
  /**
   * This String is the default message stored instead of the actual audit payload if the audit payload size
   * exceeded the maximum size limit configuration
   */
  String PAYLOAD_SKIP_MSG_FORMAT = "Payload was not stored as its size exceeds the limit [%d] configured by druid.audit.manager.maxPayloadSizeBytes";

  String X_DRUID_AUTHOR = "X-Druid-Author";

  String X_DRUID_COMMENT = "X-Druid-Comment";

  /**
   * inserts an audit entry in the Audit Table
   * @param key of the audit entry
   * @param type of the audit entry
   * @param auditInfo of the audit entry
   * @param payload of the audit entry
   * @param configSerde of the payload of the audit entry
   */
  <T> void doAudit(String key, String type, AuditInfo auditInfo, T payload, ConfigSerde<T> configSerde);

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

  /**
   * Remove audit logs created older than the given timestamp.
   *
   * @param timestamp timestamp in milliseconds
   * @return number of audit logs removed
   */
  int removeAuditLogsOlderThan(long timestamp);
}
