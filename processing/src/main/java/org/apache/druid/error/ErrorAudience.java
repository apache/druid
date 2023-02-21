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

package org.apache.druid.error;

/**
 * The set of persona (audiences) for Druid exceptions. The audience is
 * not a technical factor: it is merely a way to encourage developers to
 * think about who can act on an error message. All errors go to the user
 * who submitted the request, but perhaps in a simplified, redacted form.
 * Such messages also target the actual audience: the Druid admin, the
 * cluster admin, a Druid developer, etc.
 * <p>
 * Sometimes the target audience is not known, or is ambiguous. In that
 * case, just use {@link ErrorAudience#VARIOUS}.
 */
public enum ErrorAudience
{
  USER,
  DRUID_ADMIN,
  CLUSTER_ADMIN,
  DRUID_DEVELOPER,
  VARIOUS
}
