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

/**
 * Message relays provide a mechanism to send messages from server to client using long polling. The messages are
 * sent in order, with acknowledgements from client to server when a message has been successfully delivered.
 *
 * This is useful when there is some need for some "downstream" servers to send low-latency messages to some
 * "upstream" server, but where establishing connections from downstream servers to upstream servers would not be
 * desirable. This is typically done when upstream servers want to keep state in-memory that is updated incrementally
 * by downstream servers, and where there may be lots of instances of downstream servers.
 *
 * This structure has two main benefits. First, it prevents upstream servers from being overwhelmed by connections
 * from downstream servers. Second, it allows upstream servers to drive the updates of their own state, and better
 * handle events like restarts and leader changes.
 *
 * On the downstream (server) side, messages are placed into an {@link org.apache.druid.messages.server.Outbox}
 * and served by a {@link org.apache.druid.messages.server.MessageRelayResource}.
 *
 * On the upstream (client) side, messages are retrieved by {@link org.apache.druid.messages.client.MessageRelays}
 * using {@link org.apache.druid.messages.client.MessageRelayClient}.
 *
 * This is currently used by Dart (multi-stage-query engine running on Brokers and Historicals) to implement
 * worker-to-controller messages. In the future it may also be used to implement
 * {@link org.apache.druid.server.coordination.ChangeRequestHttpSyncer}.
 */

package org.apache.druid.messages;
