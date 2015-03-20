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

package io.druid.audit;


import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;

import java.io.IOException;
import java.util.List;

public interface AuditManager
{
  public static final String X_DRUID_AUTHOR = "X-Druid-Author";

  public static final String X_DRUID_COMMENT = "X-Druid-Comment";

  /**
   * inserts an audit Entry in the Audit Table
   * @param auditEntry
   */
  public void doAudit(AuditEntry auditEntry);

  /**
   * inserts an audit Entry in audit table using the handler provided
   * used to do the audit in same transaction as the config changes
   * @param auditEntry
   * @param handler
   * @throws IOException
   */
  public void doAudit(AuditEntry auditEntry, Handle handler) throws IOException;

  /**
   * provides audit history for given key, type and interval
   * @param key
   * @param type
   * @param interval
   * @return list of AuditEntries satisfying the passed parameters
   */
  public List<AuditEntry> fetchAuditHistory(String key, String type, Interval interval);

}
