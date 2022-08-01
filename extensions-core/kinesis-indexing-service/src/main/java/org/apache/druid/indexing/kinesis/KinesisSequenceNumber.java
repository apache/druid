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

package org.apache.druid.indexing.kinesis;


import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;

import java.math.BigInteger;

// OrderedSequenceNumber.equals() should be used instead.
@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
public class KinesisSequenceNumber extends OrderedSequenceNumber<String>
{
  /**
   * In Kinesis, when a shard is closed due to shard splitting, a null ShardIterator is returned.
   * The EOS marker is placed at the end of the Kinesis Record Supplier buffer, such that when
   * an indexing task pulls the record 'EOS', it knows the shard has been closed and should stop
   * reading and start publishing
   */
  public static final String END_OF_SHARD_MARKER = "EOS";


  /**
   *  This special marker is used by the KinesisSupervisor to set the endOffsets of newly created indexing tasks. This
   *  is necessary because streaming tasks do not have endPartitionOffsets. This marker signals to the task that it
   *  should continue to ingest data until taskDuration has elapsed or the task was stopped or paused or killed.
   */
  public static final String NO_END_SEQUENCE_NUMBER = "NO_END_SEQUENCE_NUMBER";


  /**
   * This special marker is used by the KinesisSupervisor to mark that a shard has been expired
   * (i.e., closed and then the retention period has passed)
   */
  public static final String EXPIRED_MARKER = "EXPIRED";

  /**
   * Indicates that records have not been read from a shard which needs to be processed from sequence type: TRIM_HORIZON
   */
  public static final String UNREAD_TRIM_HORIZON = "UNREAD_TRIM_HORIZON";

  /**
   * Indicates that records have not been read from a shard which needs to be processed from sequence type: LATEST
   */
  public static final String UNREAD_LATEST = "UNREAD_LATEST";

  /**
   * this flag is used to indicate either END_OF_SHARD_MARKER, NO_END_SEQUENCE_NUMBER
   * or EXPIRED_MARKER so that they can be properly compared
   * with other sequence numbers
   */
  private final boolean isMaxSequenceNumber;
  private final BigInteger intSequence;

  private KinesisSequenceNumber(String sequenceNumber, boolean isExclusive)
  {
    super(sequenceNumber, isExclusive);
    if (!isValidAWSKinesisSequence(sequenceNumber)) {
      isMaxSequenceNumber = !isUnreadSequence(sequenceNumber);
      this.intSequence = null;
    } else {
      this.intSequence = new BigInteger(sequenceNumber);
      this.isMaxSequenceNumber = false;
    }
  }

  public static KinesisSequenceNumber of(String sequenceNumber)
  {
    return new KinesisSequenceNumber(sequenceNumber, false);
  }

  public static KinesisSequenceNumber of(String sequenceNumber, boolean isExclusive)
  {
    return new KinesisSequenceNumber(sequenceNumber, isExclusive);
  }

  /**
   * Checks whether the sequence number is recognized by kinesis client library
   * @param sequenceNumber
   * @return
   */
  public static boolean isValidAWSKinesisSequence(String sequenceNumber)
  {
    return !(END_OF_SHARD_MARKER.equals(sequenceNumber)
             || NO_END_SEQUENCE_NUMBER.equals(sequenceNumber)
             || EXPIRED_MARKER.equals(sequenceNumber)
             || UNREAD_TRIM_HORIZON.equals(sequenceNumber)
             || UNREAD_LATEST.equals(sequenceNumber)
      );
  }

  @Override
  public int compareTo(OrderedSequenceNumber<String> o)
  {
    KinesisSequenceNumber num = (KinesisSequenceNumber) o;
    if (isUnread() && num.isUnread()) {
      return 0;
    } else if (isUnread()) {
      return -1;
    } else if (num.isUnread()) {
      return 1;
    }
    if (isMaxSequenceNumber && num.isMaxSequenceNumber) {
      return 0;
    } else if (isMaxSequenceNumber) {
      return 1;
    } else if (num.isMaxSequenceNumber) {
      return -1;
    }
    return this.intSequence.compareTo(new BigInteger(o.get()));
  }

  @Override
  public boolean isAvailableWithEarliest(OrderedSequenceNumber<String> earliest)
  {
    if (isUnread()) {
      return true;
    }
    return super.isAvailableWithEarliest(earliest);
  }

  @Override
  public boolean isMoreToReadBeforeReadingRecord(OrderedSequenceNumber<String> end, boolean isEndOffsetExclusive)
  {
    // Kinesis sequence number checks are exclusive for AWS numeric sequences
    // However, If a record is UNREAD and the end offset is finalized to be UNREAD, we have caught up. (inclusive)
    if (isUnreadSequence(end.get())) {
      return false;
    }
    return super.isMoreToReadBeforeReadingRecord(end, isEndOffsetExclusive);
  }

  public boolean isUnread()
  {
    return isUnreadSequence(get());
  }

  private boolean isUnreadSequence(String sequence)
  {
    return UNREAD_TRIM_HORIZON.equals(sequence) || UNREAD_LATEST.equals(sequence);
  }
}
