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

package org.apache.druid.indexing.seekablestream;

/**
 * Thrown by {@link SeekableStreamIndexTaskRunner#getRecords} when the task cannot make any further progress reading
 * the stream, for example because the offsets it must read next have been removed from the stream.
 *
 * Unlike other exceptions thrown from the read loop, the runner stops reading, publishes the data read so far and
 * then fails the task with the message of this exception. Failing the task makes the supervisor re-read the start
 * offsets from the metadata store, so the problem shows up in the supervisor status instead of the task retrying
 * silently until its duration elapses.
 */
public class UnrecoverableStreamReadException extends Exception
{
  public UnrecoverableStreamReadException(String message)
  {
    super(message);
  }
}
