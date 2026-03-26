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

package org.apache.druid.testing.embedded.tools;

import org.apache.druid.java.util.common.Pair;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import java.util.List;

/**
 * {@link EventSerializer} that serializes Wikipedia stream events as Thrift compact-encoded
 * {@link WikipediaThriftEvent} objects. All field values are converted to strings so that
 * integer values like {@code added}, {@code deleted}, and {@code delta} are serialized
 * uniformly without requiring a mixed-type Thrift struct.
 */
public class ThriftEventSerializer implements EventSerializer
{
  private static final TSerializer SERIALIZER = new TSerializer(new TCompactProtocol.Factory());

  @Override
  public byte[] serialize(List<Pair<String, Object>> event)
  {
    WikipediaThriftEvent wikiEvent = new WikipediaThriftEvent();
    for (Pair<String, Object> pair : event) {
      String value = pair.rhs == null ? null : String.valueOf(pair.rhs);
      switch (pair.lhs) {
        case "timestamp":
          wikiEvent.timestamp = value;
          break;
        case "page":
          wikiEvent.page = value;
          break;
        case "language":
          wikiEvent.language = value;
          break;
        case "user":
          wikiEvent.user = value;
          break;
        case "unpatrolled":
          wikiEvent.unpatrolled = value;
          break;
        case "newPage":
          wikiEvent.newPage = value;
          break;
        case "robot":
          wikiEvent.robot = value;
          break;
        case "anonymous":
          wikiEvent.anonymous = value;
          break;
        case "namespace":
          wikiEvent.namespace = value;
          break;
        case "continent":
          wikiEvent.continent = value;
          break;
        case "country":
          wikiEvent.country = value;
          break;
        case "region":
          wikiEvent.region = value;
          break;
        case "city":
          wikiEvent.city = value;
          break;
        case "added":
          wikiEvent.added = value;
          break;
        case "deleted":
          wikiEvent.deleted = value;
          break;
        case "delta":
          wikiEvent.delta = value;
          break;
        default:
          break;
      }
    }
    try {
      return SERIALIZER.serialize(wikiEvent);
    }
    catch (TException e) {
      throw new RuntimeException("Failed to serialize WikipediaThriftEvent", e);
    }
  }

  @Override
  public void close()
  {
  }
}
