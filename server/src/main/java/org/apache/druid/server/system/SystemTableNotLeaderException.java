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

package org.apache.druid.server.system;

import org.apache.druid.java.util.common.ISE;

/**
 * Signals that a leader-only system table request reached a node that is no longer the leader.
 *
 * <p>The Broker may discover a leader and then contact it after leadership has changed. The native-query error
 * response preserves this exception's class name, allowing {@code SystemTableQueryClient} to distinguish that race
 * from an ordinary query failure, resolve the new leader, and retry once. A generic exception would not provide a
 * reliable retry signal and retrying every failure could hide genuine query errors.</p>
 */
public class SystemTableNotLeaderException extends ISE
{
  public SystemTableNotLeaderException(final String nodeRole)
  {
    super("Node role[%s] is not the current leader", nodeRole);
  }
}
