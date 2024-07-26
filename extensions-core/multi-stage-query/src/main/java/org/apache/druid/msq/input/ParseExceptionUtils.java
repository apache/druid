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

package org.apache.druid.msq.input;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.input.external.ExternalSegment;
import org.apache.druid.msq.input.inline.InlineInputSliceReader;
import org.apache.druid.query.lookup.LookupSegment;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;

import javax.annotation.Nullable;

/**
 * Utility class containing methods that help in generating the {@link org.apache.druid.sql.calcite.parser.ParseException}
 * in the frame processors
 */
public class ParseExceptionUtils
{

  /**
   * Given a segment, this returns the human-readable description of the segment which can allow user to figure out the
   * source of the parse exception
   */
  @Nullable
  public static String generateReadableInputSourceNameFromMappedSegment(Segment segment)
  {
    if (segment instanceof ExternalSegment) {
      return StringUtils.format(
          "external input source: %s",
          ((ExternalSegment) segment).externalInputSource().toString()
      );
    } else if (segment instanceof LookupSegment) {
      return StringUtils.format("lookup input source: %s", segment.getId().getDataSource());
    } else if (segment instanceof QueryableIndexSegment) {
      return StringUtils.format("table input source: %s", segment.getId().getDataSource());
    } else if (InlineInputSliceReader.SEGMENT_ID.equals(segment.getId().getDataSource())) {
      return "inline input source";
    }

    return null;
  }
}
