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

package org.apache.druid.indexing.pulsar;

import com.google.common.collect.ComparisonChain;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;

import javax.validation.constraints.NotNull;

// OrderedSequenceNumber.equals() should be used instead.
@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
public class PulsarSequenceNumber extends OrderedSequenceNumber<String>
{
  private PulsarSequenceNumber(String sequenceNumber)
  {
    super(sequenceNumber, false);
  }

  public static PulsarSequenceNumber of(String sequenceNumber)
  {
    return new PulsarSequenceNumber(sequenceNumber);
  }

  @Override
  public int compareTo(
      @NotNull OrderedSequenceNumber<String> o
  )
  {
    String[] ss1 = this.get().split(":");
    String[] ss2 = o.get().split(":");
    return ComparisonChain.start()
        .compare(Long.parseLong(ss1[0]), Long.parseLong(ss2[0]))
        .compare(Long.parseLong(ss1[1]), Long.parseLong(ss2[1]))
        .compare(Integer.parseInt(ss1[2]), Integer.parseInt(ss2[2]))
        .result();
  }

}
