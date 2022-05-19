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

package org.apache.druid.data.input.kafkainput;

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class KafkaStringHeaderReader implements KafkaHeaderReader
{
  private static final Logger log = new Logger(KafkaStringHeaderReader.class);
  private final Headers headers;
  private final String headerLabelPrefix;
  private final Charset encoding;

  public KafkaStringHeaderReader(Headers headers,
                                 String headerLabelPrefix,
                                 Charset encoding)
  {
    this.headers = headers;
    this.headerLabelPrefix = headerLabelPrefix;
    this.encoding = encoding;
  }

  @Override
  public List<Pair<String, Object>> read()
  {
    List<Pair<String, Object>> events = new ArrayList<>();
    for (Header hdr : headers) {
      String s = new String(hdr.value(), this.encoding);
      String newKey = this.headerLabelPrefix + hdr.key();
      events.add(Pair.of(newKey, s));
    }
    return events;
  }
}
