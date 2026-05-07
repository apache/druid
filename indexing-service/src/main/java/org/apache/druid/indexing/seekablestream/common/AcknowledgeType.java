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

package org.apache.druid.indexing.seekablestream.common;

/**
 * Acknowledgement types for queue-semantics record suppliers where the broker
 * manages delivery state (e.g., Kafka Share Groups).
 *
 * Maps directly to Kafka's {@code AcknowledgeType} enum values.
 */
public enum AcknowledgeType
{
  /**
   * The record was consumed and processed successfully.
   * The broker will mark the record as committed.
   */
  ACCEPT,

  /**
   * Release the record for redelivery to another consumer.
   * Used when the consumer cannot process the record right now
   * but another consumer might be able to.
   */
  RELEASE,

  /**
   * Reject the record permanently. The broker will not redeliver it.
   * Used for poison-pill records that can never be processed.
   */
  REJECT,

  /**
   * Extend the acquisition lock on the record without releasing or accepting it.
   * Used by background lock-renewal workers to keep records under lock while
   * the foreground processing thread continues building segments. Maps to
   * Kafka's {@code AcknowledgeType.RENEW}.
   */
  RENEW
}
