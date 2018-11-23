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

package org.apache.druid.server.metrics;

/**
 * An EventReceiverFirehoseMetric is an object with metrics about EventReceiverFirehose objects.
 * It is not likely that anything other than an EventReceiverFirehose actually implements this.
 * This interface is not part of the public API and backwards incompatible changes can occur without
 * requiring a major (or even minor) version change.
 * The interface's primary purpose is to be able to share metrics via the EventReceiverFirehoseRegister
 * without exposing the entire EventReceiverFirehose
 */
public interface EventReceiverFirehoseMetric
{
  /**
   * Return the current number of {@link org.apache.druid.data.input.InputRow} that are stored in the buffer.
   */
  int getCurrentBufferSize();

  /**
   * Return the capacity of the buffer.
   */
  int getCapacity();

  /**
   * Return the number of bytes received by the firehose.
   */
  long getBytesReceived();


}
