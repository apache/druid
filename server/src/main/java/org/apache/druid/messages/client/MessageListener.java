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

package org.apache.druid.messages.client;

import org.apache.druid.server.DruidNode;

/**
 * Listener for messages received by clients.
 */
public interface MessageListener<MessageType>
{
  /**
   * Called when a server is added.
   *
   * @param node server node
   */
  void serverAdded(DruidNode node);

  /**
   * Called when a message is received. Should not throw exceptions. If this method does throw an exception,
   * the exception is logged and the message is acknowledged anyway.
   *
   * @param message the message that was received
   */
  void messageReceived(MessageType message);

  /**
   * Called when a server is removed.
   *
   * @param node server node
   */
  void serverRemoved(DruidNode node);
}
