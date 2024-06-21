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

package org.apache.druid.msq.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.joda.time.DateTime;

import java.util.Objects;

@JsonTypeName(TooManySegmentsFault.CODE)
public class TooManySegmentsFault extends BaseMSQFault
{
  public static final String CODE = "TooManySegments";

  private final DateTime timeChunk;
  private final int numSegments;
  private final int maxNumSegments;

  @JsonCreator
  public TooManySegmentsFault(
      @JsonProperty("timeChunk") final DateTime timeChunk,
      @JsonProperty("numSegments") final int numSegments,
      @JsonProperty("maxNumSegments") final int maxNumSegments
  )
  {
    super(CODE, "Too many segments [%,d] generated in time chunk[%s] (maxNumSegments = [%,d]). "
                + "Please increase maxNumSegments in the query context or use a finer PARTITIONED BY granularity.",
          numSegments, timeChunk, maxNumSegments);
    this.timeChunk = timeChunk;
    this.numSegments = numSegments;
    this.maxNumSegments = maxNumSegments;
  }

  @JsonProperty
  public DateTime getTimeChunk()
  {
    return timeChunk;
  }

  @JsonProperty
  public int getNumSegments()
  {
    return numSegments;
  }

  @JsonProperty
  public int getMaxNumSegments()
  {
    return maxNumSegments;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TooManySegmentsFault that = (TooManySegmentsFault) o;
    return numSegments == that.numSegments
           && maxNumSegments == that.maxNumSegments
           && Objects.equals(timeChunk, that.timeChunk);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), timeChunk, numSegments, maxNumSegments);
  }
}
