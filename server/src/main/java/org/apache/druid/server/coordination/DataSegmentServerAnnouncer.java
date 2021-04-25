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

package org.apache.druid.server.coordination;

/**
 * We are gradually migrating usages of this to {@link org.apache.druid.discovery.DruidNodeAnnouncer}.
 *
 * However, it's still required in some cases. As of this writing (2020-12-03) it's required for any process that
 * is serving queryable segments via Curator-based segment discovery. (When using Curator for segment discovery, Brokers
 * look for these announcements as part of discovering what segments are available.)
 */
@Deprecated
public interface DataSegmentServerAnnouncer
{
  void announce();
  void unannounce();

  class Noop implements DataSegmentServerAnnouncer
  {

    @Override
    public void announce()
    {

    }

    @Override
    public void unannounce()
    {

    }
  }
}
