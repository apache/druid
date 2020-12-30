<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->
lookup-aware-server-selector
=============

Overview
=============
Servers are selected to query for particular segments in CachingClusteredClient using a ServerSelectorStrategy. This extension proposes a new FilteringServerSelectorStrategy that filters the servers provided by the TierSelectorStrategy and then applies another ServerSelectorStrategy (such as the existing random or connection count strategies). A new component, LookupStatusView is added to poll the nodeStatus endpoint on the coordinator for lookup status.

This package provides a filter for unavailable lookups. Given a query with lookups, one server with a missing lookup can cause query execution to fail. When the broker distributes a query to historicals and realtime servers, if any one of those servers does not have the lookup, the query fails as a whole. Lookups can fail to load for a number of reasons, such as missing firewall rules, drivers or slow loading times for large, frequently updated lookups. These queries could be served if the broker considered lookup status when selecting servers for querying.

To enable this ServerSelectorStrategy, set the balancer type to be filter and then select another strategy to select a server from the filtered servers. Filters are specified in a list. Extensions to this strategy can provide additional filters, such as for server reliability by binding a named provider for a ServerFilterStrategy. 

druid.broker.balancer.type=filter
druid.broker.balancer.filter.after.type=random
druid.broker.balancer.filter.filters=["lookup-aware","no-op"]

