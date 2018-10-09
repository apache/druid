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

import javax.validation.constraints.NotNull;
import java.math.BigInteger;
import java.util.Objects;

public class KinesisSequenceNumber extends OrderedSequenceNumber<String>
{

  private final BigInteger intSequence;

  private KinesisSequenceNumber(@NotNull String sequenceNumber, boolean useExclusive, boolean isExclusive)
  {
    super(sequenceNumber, useExclusive, isExclusive);
    this.intSequence = "".equals(sequenceNumber) ? new BigInteger("-1") : new BigInteger(sequenceNumber);
  }

  public static KinesisSequenceNumber of(String sequenceNumber)
  {
    return new KinesisSequenceNumber(sequenceNumber, false, false);
  }

  public static KinesisSequenceNumber of(String sequenceNumber, boolean useExclusive, boolean isExclusive)
  {
    return new KinesisSequenceNumber(sequenceNumber, useExclusive, isExclusive);
  }

  public BigInteger getBigInteger()
  {
    return intSequence;
  }

  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof KinesisSequenceNumber)) {
      return false;
    }
    return this.compareTo((KinesisSequenceNumber) o) == 0;
  }


  @Override
  public int compareTo(@NotNull OrderedSequenceNumber<String> o)
  {
    return this.intSequence.compareTo(new BigInteger(o.get()));
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), intSequence);
  }
}
