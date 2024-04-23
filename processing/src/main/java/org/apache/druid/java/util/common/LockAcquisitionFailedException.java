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

package org.apache.druid.java.util.common;

import org.joda.time.Interval;

/**
 * Thrown to indicate that a lock acquisition has failed.
 * <p>
 * This exception is a specialized form of {@link IllegalStateException}
 * and is thrown when an operation fails due to an inability to acquire
 * a necessary lock.
 * </p>
 */
public class LockAcquisitionFailedException extends ISE
{
  public LockAcquisitionFailedException(String formatText, Object... arguments)
  {
    super(formatText, arguments);
  }

  public LockAcquisitionFailedException(Interval interval)
  {
    this(StringUtils.format("Lock for interval [%s] was revoked", interval));
  }
}
