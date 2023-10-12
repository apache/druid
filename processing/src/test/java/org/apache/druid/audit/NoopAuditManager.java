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

import java.util.List;

public class NoopAuditManager implements AuditManager
{
  @Override
  public <T> void doAudit(String key, String type, AuditInfo auditInfo, T payload, ConfigSerde<T> configSerde)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void doAudit(AuditEntry auditEntry, Handle handler)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<AuditEntry> fetchAuditHistory(String key, String type, Interval interval)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<AuditEntry> fetchAuditHistory(String type, Interval interval)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<AuditEntry> fetchAuditHistory(String key, String type, int limit)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<AuditEntry> fetchAuditHistory(String type, int limit)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int removeAuditLogsOlderThan(long timestamp)
  {
    throw new UnsupportedOperationException();
  }
}
