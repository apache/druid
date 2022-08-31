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

package org.apache.druid.msq.kernel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Strings;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Comparator;
import java.util.Objects;

/**
 * Globally unique stage identifier: query ID plus stage number.
 */
public class StageId implements Comparable<StageId>
{
  private static final Comparator<StageId> COMPARATOR =
      Comparator.comparing(StageId::getQueryId)
                .thenComparing(StageId::getStageNumber);

  private final String queryId;
  private final int stageNumber;

  public StageId(final String queryId, final int stageNumber)
  {
    if (Strings.isNullOrEmpty(queryId)) {
      throw new IAE("Null or empty queryId");
    }

    if (stageNumber < 0) {
      throw new IAE("Invalid stageNumber [%s]", stageNumber);
    }

    this.queryId = queryId;
    this.stageNumber = stageNumber;
  }

  @JsonCreator
  public static StageId fromString(final String s)
  {
    final int lastUnderscore = s.lastIndexOf('_');

    if (lastUnderscore > 0 && lastUnderscore < s.length() - 1) {
      final Long stageNumber = GuavaUtils.tryParseLong(s.substring(lastUnderscore + 1));

      if (stageNumber != null && stageNumber >= 0 && stageNumber <= Integer.MAX_VALUE) {
        return new StageId(s.substring(0, lastUnderscore), stageNumber.intValue());
      }
    }

    throw new IAE("Not a valid stage id: [%s]", s);
  }

  public String getQueryId()
  {
    return queryId;
  }

  public int getStageNumber()
  {
    return stageNumber;
  }

  @Override
  public int compareTo(StageId that)
  {
    return COMPARATOR.compare(this, that);
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
    StageId stageId = (StageId) o;
    return stageNumber == stageId.stageNumber && Objects.equals(queryId, stageId.queryId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(queryId, stageNumber);
  }

  @Override
  @JsonValue
  public String toString()
  {
    return StringUtils.format("%s_%s", queryId, stageNumber);
  }
}
