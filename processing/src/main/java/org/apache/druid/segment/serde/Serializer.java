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

package org.apache.druid.segment.serde;

import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * Main interface for "serializeable something" in Druid segment serialization.
 */
public interface Serializer
{
  /**
   * Returns the number of bytes, that this Serializer will write to the output _channel_ (not smoosher) on a {@link
   * #writeTo} call.
   */
  long getSerializedSize() throws IOException;

  /**
   * Writes serialized form of this object to the given channel. If parallel data streams are needed, they could be
   * created with the provided smoosher.
   */
  void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException;
}
