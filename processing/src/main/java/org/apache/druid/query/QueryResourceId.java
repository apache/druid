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

package org.apache.druid.query;

import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * Wrapper class on the queryResourceId string. The object must be addressable on an associative map, therefore it must implement
 * equals and hashCode.
 * <p>
 * Query's resource id is used to allocate the resources, and identify the resources allocated to a query in a global pool.
 * Queries USUALLY do not share any resources - each query is assigned its own thread, and buffer pool. However, some resources
 * are shared globally - the GroupBy query's merge buffers being a prime example of those (and the primary utiliser of the
 * query's resource id). Such resources MUST be allocated once to prevent deadlocks, and can be used throughout the query stack, till
 * the query holds those resources, or till its completion. A query holding a global resources must not request for more of the same
 * resource, or else it becomes a candidate for deadlocks.
 * <p>
 * Each query has a unique resource id, that is assigned to it when it enters the queryable server. This is distinct from
 * the existing queryId, subqueryId and sqlQueryId in the following ways:
 * 1. It is not assigned by the user, it is assigned internally for usage by the Druid server
 * 2. The query's resource id will be unique to the query in the system. The queryId can be non-unique amongst the queries
 * that are running in the system. Druid must ensure that the queryResourceId isn't unique. If the user (somehow)
 * assigns the queryResourceId to the query, it must be overwritten internally.
 * 3. During the query server <-> data server communication, the queryResourceId assigned to a particular query can (and will)
 * differ in the query servers and the data servers. This is particularly helpful in case of union queries, where a
 * single query in the broker can be treated as two separate queries and executed simultaneously in the historicals.
 * <p>
 * The queryId is assigned to the query, and populated in the query context at the time it hits the queryable server. In Druid,
 * there are three queryable servers (classes are not linkable from this method):
 * 1. {@link org.apache.druid.server.ClientQuerySegmentWalker} - For brokers
 * 2. {@link org.apache.druid.server.coordination.ServerManager} - For historicals
 * 3. {@link org.apache.druid.segment.realtime.appenderator.SinkQuerySegmentWalker} - For peons & indexer's tasks
 * <p>
 * These three classes are one of the first places the query reaches when it begins processing, therefore it is
 * guaranteed that if the resource id is allotted at only these places, no one will overwrite the resource id
 * during the execution.
 * <p>
 * Note: Historicals and Peons could have used the same query id allotted by the brokers, however they assign their own because:
 * 1. The user can directly choose to query the data server (while debugging etc.)
 * 2. UNIONs are treated as multiple separate queries when the broker sends them to the historicals. Therefore, we
 * require a unique id for each part of the union, and hence we need to reassign the resource id to the query's part,
 * or else they'll end up sharing the same resource id, as mentioned before
 * <p>
 * Notable places where QueryResourceId is used:
 * <p>
 * 1. {@link org.apache.druid.query.groupby.GroupByResourcesReservationPool} Primary user of the query resource id.
 * <p>
 * 2. {@link org.apache.druid.server.ClientQuerySegmentWalker} Allocates the query resource id on the brokers
 * <p>
 * 3. {@link org.apache.druid.server.coordination.ServerManager} Allocates the query resource id on the historicals
 * <p>
 * 4. {@link org.apache.druid.segment.realtime.appenderator.SinkQuerySegmentWalker} Allocates the query resource id on the peons
 * (MMs) and indexers
 * <p>
 * 5. {@link org.apache.druid.server.ResourceIdPopulatingQueryRunner} Populates the query resource id. ({@link org.apache.druid.server.ClientQuerySegmentWalker}
 * allocates the query resource id directly, since it also does a bunch of transforms to the query)
 * <p>
 * 6. {@link org.apache.druid.query.groupby.GroupByQueryQueryToolChest} Allocates, and associates one of the global resources,
 * merge buffers, with the query's resource id. It also cleans it up, once the query is completed. Apart from that,
 * it is also a consumer of the merge buffers it allocates.
 * <p>
 * 7. {@link org.apache.druid.query.groupby.epinephelinae.GroupByMergingQueryRunner} One of the consumer of the merge buffers,
 * allocated at the beginning of the query
 *
 * @see org.apache.druid.query.groupby.GroupByResourcesReservationPool
 */
public class QueryResourceId
{
  private final String queryResourceId;

  public QueryResourceId(String queryResourceId)
  {
    this.queryResourceId = Preconditions.checkNotNull(queryResourceId, "queryResourceId must be present");
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryResourceId that = (QueryResourceId) o;
    return Objects.equals(queryResourceId, that.queryResourceId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(queryResourceId);
  }

  @Override
  public String toString()
  {
    return queryResourceId;
  }
}
