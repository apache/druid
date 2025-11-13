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

package org.apache.druid.indexing.kafka;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.druid.indexing.kafka.supervisor.KafkaHeaderBasedFilterConfig;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.filter.Filter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Evaluates Kafka header filters for pre-ingestion filtering.
 */
public class KafkaHeaderBasedFilterEvaluator
{
  private static final Logger log = new Logger(KafkaHeaderBasedFilterEvaluator.class);

  private final HeaderFilterHandler filterHandler;
  private final String headerName;
  private final Charset encoding;
  private final Cache<ByteBuffer, String> stringDecodingCache;

  /**
   * Creates a new KafkaHeaderBasedFilterEvaluator with the given configuration.
   *
   * @param headerBasedFilterConfig the configuration containing filter, encoding, and cache settings
   * @throws IllegalArgumentException if the filter type is not supported
   */
  public KafkaHeaderBasedFilterEvaluator(KafkaHeaderBasedFilterConfig headerBasedFilterConfig)
  {
    this.encoding = Charset.forName(headerBasedFilterConfig.getEncoding());
    this.stringDecodingCache = Caffeine.newBuilder()
        .maximumSize(headerBasedFilterConfig.getStringDecodingCacheSize())
        .build();

    Filter filter = headerBasedFilterConfig.getFilter().toFilter();
    this.filterHandler = HeaderFilterHandlerFactory.forFilter(filter);
    this.headerName = filterHandler.getHeaderName();

    log.info("Initialized Kafka header filter: %s with encoding [%s] and cache size [%d]",
            filterHandler.getDescription(),
            headerBasedFilterConfig.getEncoding(),
            headerBasedFilterConfig.getStringDecodingCacheSize());
  }


  /**
   * Evaluates whether a Kafka record should be included based on its headers.
   *
   * @param record the Kafka consumer record
   * @return true if the record should be included, false if it should be filtered out
   */
  public boolean shouldIncludeRecord(ConsumerRecord<byte[], byte[]> record)
  {
    try {
      return evaluateInclusion(record.headers());
    }
    catch (Exception e) {
      log.warn(
          e,
          "Error evaluating header filter for record at topic [%s] partition [%d] offset [%d], including record",
          record.topic(),
          record.partition(),
          record.offset()
      );
      return true; // Default to including record on error
    }
  }

  /**
   * Evaluates whether a record should be included based on its headers.
   * 
   * Uses permissive behavior: records with missing, null, or undecodable headers
   * are included by default. Only records with successfully decoded header values
   * that don't match the filter criteria are excluded.
   * 
   * @param headers the Kafka message headers to evaluate
   * @return true if the record should be included, false if it should be filtered out
   */
  private boolean evaluateInclusion(Headers headers)
  {
    // Permissive behavior: missing headers result in inclusion
    if (headers == null) {
      return true;
    }

    Header header = headers.lastHeader(headerName);
    
    // Permissive behavior: header is null or empty
    if (header == null || header.value() == null) {
      return true;
    }

    String headerValue = getDecodedHeaderValue(header.value());
    // Permissive behavior: failed to decode header value
    if (headerValue == null) {
      return true;
    }

    return filterHandler.shouldInclude(headerValue);
  }


  /**
   * Decode header bytes to string with caching.
   * Returns null if decoding fails.
   */
  @Nullable
  private String getDecodedHeaderValue(byte[] headerBytes)
  {
    try {
      ByteBuffer key = ByteBuffer.wrap(headerBytes);
      return stringDecodingCache.get(key, k -> new String(headerBytes, encoding));
    }
    catch (Exception e) {
      log.warn(e, "Failed to decode header bytes, treating as null");
      return null;
    }
  }

}
