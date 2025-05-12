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

package org.apache.druid.curator.announcement;

/**
 * The {@link Announceable} is a representation of an announcement to be made in ZooKeeper.
 */
class Announceable
{
  /**
   * Represents the path in ZooKeeper where the announcement will be made.
   */
  final String path;

  /**
   * Holds the actual data to be announced.
   */
  final byte[] bytes;

  /**
   * Indicates whether parent nodes should be removed if the announcement is created successfully.
   * This can be useful for cleaning up unused paths in ZooKeeper.
   */
  final boolean removeParentsIfCreated;

  public Announceable(String path, byte[] bytes, boolean removeParentsIfCreated)
  {
    this.path = path;
    this.bytes = bytes;
    this.removeParentsIfCreated = removeParentsIfCreated;
  }

  // This should be used for updates only, where removeParentsIfCreated is not relevant.
  public Announceable(String path, byte[] bytes)
  {
    // removeParentsIfCreated is irrelevant, so we can use dummy value "false".
    this(path, bytes, false);
  }
}
