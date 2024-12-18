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

package org.apache.druid.server.security;

import com.google.common.base.Strings;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.filter.DimFilter;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

public class Access
{
  public static final String DEFAULT_ERROR_MESSAGE = "Unauthorized";
  public static final String DEFAULT_AUTHORIZED_MESSAGE = "Authorized";

  public static final Access OK = allow();
  public static final Access DENIED = deny("");

  private final boolean allowed;
  private final String message;
  // A row-level policy filter on top of table-level read access. It should be empty if there are no policy restrictions
  // or if access is requested for an action other than reading the table.
  private final Optional<DimFilter> rowFilter;

  /**
   * @deprecated use {@link #allow()} or {@link #deny(String)} instead
   */
  @Deprecated
  public Access(boolean allowed)
  {
    this(allowed, "", Optional.empty());
  }

  Access(boolean allowed, String message, Optional<DimFilter> rowFilter)
  {
    this.allowed = allowed;
    this.message = message;
    this.rowFilter = rowFilter;
  }

  public static Access allow()
  {
    return new Access(true, "", Optional.empty());
  }

  public static Access deny(@Nullable String message)
  {
    return new Access(false, Objects.isNull(message) ? "" : message, Optional.empty());
  }

  public static Access allowWithRestriction(Optional<DimFilter> rowFilter)
  {
    return new Access(true, "", rowFilter);
  }

  public boolean isAllowed()
  {
    return allowed;
  }

  public Optional<DimFilter> getRowFilter()
  {
    return rowFilter;
  }

  public String getMessage()
  {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(allowed ? DEFAULT_AUTHORIZED_MESSAGE : DEFAULT_ERROR_MESSAGE);
    if (!Strings.isNullOrEmpty(message)) {
      stringBuilder.append(", ");
      stringBuilder.append(message);
    }
    if (allowed && rowFilter.isPresent()) {
      stringBuilder.append(", with restriction ");
      stringBuilder.append(rowFilter.get());
    }
    return stringBuilder.toString();
  }

  @Override
  public String toString()
  {
    return StringUtils.format("Allowed:%s, Message:%s, Row filter: %s", allowed, message, rowFilter);
  }

}
