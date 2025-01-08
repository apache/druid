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
import org.apache.druid.query.policy.Policy;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Represents the outcome of verifying permissions to perform an {@link Action} on a {@link Resource}, along with any
 * policy restrictions.
 */
public class Access
{
  public static final String DEFAULT_ERROR_MESSAGE = "Unauthorized";
  public static final String DEFAULT_AUTHORIZED_MESSAGE = "Authorized";

  public static final Access OK = allow();
  public static final Access DENIED = deny("");

  private final boolean allowed;
  private final String message;
  // A policy restriction on top of table-level read access. It should be empty if there are no policy restrictions
  // or if access is requested for an action other than reading the table.
  private final Optional<Policy> policy;

  /**
   * @deprecated use {@link #allow()} or {@link #deny(String)} instead
   */
  @Deprecated
  public Access(boolean allowed)
  {
    this(allowed, "", Optional.empty());
  }

  /**
   * @deprecated use {@link #allow()} or {@link #deny(String)} instead
   */
  @Deprecated
  public Access(boolean allowed, String message)
  {
    this(allowed, message, Optional.empty());
  }

  Access(boolean allowed, String message, Optional<Policy> policy)
  {
    this.allowed = allowed;
    this.message = message;
    this.policy = policy;
  }

  /**
   * Constructs {@link Access} instance with access allowed, with no policy restriction.
   */
  public static Access allow()
  {
    return new Access(true, "", Optional.empty());
  }

  /**
   * Contructs {@link Access} instance with access denied.
   */
  public static Access deny(@Nullable String message)
  {
    return new Access(false, StringUtils.nullToEmptyNonDruidDataString(message), null);
  }

  /**
   * Contructs {@link Access} instance with access allowed, but with policy restriction.
   */
  public static Access allowWithRestriction(Policy policy)
  {
    return new Access(true, "", Optional.of(policy));
  }

  /**
   * Returns true if access allowed, ignoring any policy restrictions.
   */
  public boolean isAllowed()
  {
    return allowed;
  }

  public Optional<Policy> getPolicy()
  {
    return policy;
  }

  public String getMessage()
  {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(allowed ? DEFAULT_AUTHORIZED_MESSAGE : DEFAULT_ERROR_MESSAGE);
    if (!Strings.isNullOrEmpty(message)) {
      stringBuilder.append(", ");
      stringBuilder.append(message);
    }
    if (allowed && policy.isPresent()) {
      stringBuilder.append(", with restriction [");
      stringBuilder.append(policy.get());
      stringBuilder.append("]");
    }
    return stringBuilder.toString();
  }

  @Override
  public String toString()
  {
    return StringUtils.format("Allowed:%s, Message:%s, Policy: %s", allowed, message, policy);
  }

}
