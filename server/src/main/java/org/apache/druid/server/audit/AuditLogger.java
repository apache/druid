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

package org.apache.druid.server.audit;

import org.apache.druid.audit.AuditEntry;
import org.apache.druid.java.util.common.logger.Logger;

public class AuditLogger
{
  public enum Level
  {
    DEBUG, INFO, WARN
  }

  private static final String MSG_FORMAT
      = "User[%s], identity[%s], IP[%s] performed action of type[%s] on key[%s]"
        + " with comment[%s], request[%s], payload[%s].";

  private final Level level;
  private final Logger logger = new Logger(AuditLogger.class);

  public AuditLogger(Level level)
  {
    this.level = level;
  }

  public void log(AuditEntry entry)
  {
    Object[] args = {
        entry.getAuditInfo().getAuthor(),
        entry.getAuditInfo().getIdentity(),
        entry.getAuditInfo().getIp(),
        entry.getType(),
        entry.getKey(),
        entry.getAuditInfo().getComment(),
        entry.getRequest(),
        entry.getPayload()
    };
    switch (level) {
      case DEBUG:
        logger.debug(MSG_FORMAT, args);
        break;
      case INFO:
        logger.info(MSG_FORMAT, args);
        break;
      case WARN:
        logger.warn(MSG_FORMAT, args);
        break;
    }
  }
}
