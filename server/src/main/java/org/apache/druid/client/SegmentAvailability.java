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
 * What a Broker believes about a segment that currently has no server to query.
 *
 * @see org.apache.druid.client.selector.ServerSelector#getAvailability()
 */
public enum SegmentAvailability
{
  /**
   * The segment has at least one server, or the Broker has not yet established why it has none. Queries must not be
   * failed on this basis: it covers both the normal case and the window in which the Coordinator has not answered
   * yet, and failing during that window would turn a Coordinator hiccup into query errors.
   */
  UNKNOWN,

  /**
   * The Coordinator says the segment is used and its rules ask for at least one replica, so some server ought to be
   * serving it. Having no server for it means results would silently be incomplete.
   */
  EXPECTED_AVAILABLE,

  /**
   * The Coordinator says the segment is unused, or its rules ask for zero replicas so it is queryable only from deep
   * storage. Its absence from the timeline is expected and must not be reported.
   */
  NOT_EXPECTED
}
