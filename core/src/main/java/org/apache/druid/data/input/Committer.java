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

package org.apache.druid.data.input;

import org.apache.druid.guice.annotations.ExtensionPoint;

import javax.annotation.Nullable;

/**
 * Committer includes a Runnable and a Jackson-serialized metadata object containing the offset
 */
@ExtensionPoint
public interface Committer extends Runnable
{
  /**
   * @return A json serialized representation of commit metadata,
   * which needs to be serialized and deserialized by Jackson.
   * Commit metadata can be a complex type, but we recommend keeping it to List/Map/"Primitive JSON" types
   */
  @Nullable
  Object getMetadata();
}
