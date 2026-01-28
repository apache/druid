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

package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility to generate schema fingerprint which is used to ensure schema uniqueness in the metadata database.
 * Note, that the generated fingerprint is independent of the column order.
 */
@LazySingleton
public class FingerprintGenerator
{
  private static final Logger log = new Logger(FingerprintGenerator.class);

  private final ObjectMapper objectMapper;

  @Inject
  public FingerprintGenerator(ObjectMapper objectMapper)
  {
    this.objectMapper = objectMapper;
  }

  /**
   * Generates fingerprint or hash string for an object using SHA-256 hash algorithm.
   */
  @SuppressWarnings("UnstableApiUsage")
  public String generateFingerprint(final SchemaPayload schemaPayload, final String dataSource, final int version)
  {
    // Sort the column names in lexicographic order
    // The aggregator factories are column order independent since they are stored in a hashmap
    // This ensures that all permutations of a given columns would result in the same fingerprint
    // thus avoiding schema explosion in the metadata database
    // Note that this signature is not persisted anywhere, it is only used for fingerprint computation
    final RowSignature sortedSignature = getLexicographicallySortedSignature(schemaPayload.getRowSignature());
    final SchemaPayload updatedPayload = new SchemaPayload(sortedSignature, schemaPayload.getAggregatorFactories());
    try {

      final Hasher hasher = Hashing.sha256().newHasher();

      hasher.putBytes(objectMapper.writeValueAsBytes(updatedPayload));
      // add delimiter, inspired from org.apache.druid.metadata.PendingSegmentRecord.computeSequenceNamePrevIdSha1
      hasher.putByte((byte) 0xff);

      hasher.putBytes(StringUtils.toUtf8(dataSource));
      hasher.putByte((byte) 0xff);

      hasher.putBytes(Ints.toByteArray(version));
      hasher.putByte((byte) 0xff);

      return BaseEncoding.base16().encode(hasher.hash().asBytes());
    }
    catch (IOException e) {
      log.error(
          e,
          "Exception generating schema fingerprint (version[%d]) for datasource[%s], payload[%s].",
          version, dataSource, schemaPayload
      );
      throw DruidException.defensive(
          "Could not generate schema fingerprint (version[%d]) for datasource[%s].",
          dataSource, version
      );
    }
  }

  @VisibleForTesting
  protected RowSignature getLexicographicallySortedSignature(final RowSignature rowSignature)
  {
    final List<String> columns = new ArrayList<>(rowSignature.getColumnNames());

    Collections.sort(columns);

    final RowSignature.Builder sortedSignature = RowSignature.builder();

    for (String column : columns) {
      ColumnType type = rowSignature.getColumnType(column).orElse(null);
      sortedSignature.add(column, type);
    }

    return sortedSignature.build();
  }
}
