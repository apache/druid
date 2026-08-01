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

package org.apache.druid.client;

/**
 * What a Broker does when a query touches a segment that ought to be available but has no server.
 *
 * @see BrokerSegmentWatcherConfig#getUnavailableSegmentPolicy()
 */
public enum UnavailableSegmentPolicy
{
  /**
   * Behave as Druid did before unavailability detection existed: drop the segment from the timeline as soon as its
   * last server goes away, and silently omit it from query results.
   */
  IGNORE,

  /**
   * Detect and report unavailable segments through logs and metrics, but still answer queries with whatever data is
   * available. The default, so that turning detection on does not by itself start failing queries.
   */
  ALERT,

  /**
   * Fail queries that touch an unavailable segment, rather than returning results that are silently incomplete.
   * Honours {@code druid.query.retryPolicy.numTries} and the per-query partial-results context flag.
   */
  FAIL
}
