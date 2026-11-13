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

package org.apache.druid.iceberg.input;

import org.apache.iceberg.Files;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * A read-only Iceberg {@link FileIO} implementation used on worker nodes to open Iceberg data
 * and delete files during V2 ingestion.
 *
 * <p><b>Phase 1 scope — local filesystem only.</b>
 * For local paths (no URI scheme, or {@code file://} prefix), this class delegates to
 * Iceberg's built-in {@link Files#localInput(String)}, which is backed by
 * {@link java.io.RandomAccessFile}. No custom seekable-stream implementation is needed;
 * Iceberg provides this internally.
 *
 * <p>For non-local paths ({@code s3://}, {@code hdfs://}, etc.) this class throws
 * {@link UnsupportedOperationException}. Workers handling such paths fall back to the
 * catalog's own {@link FileIO} — the same behaviour as before this change. Full
 * {@code WarehouseFileIO} support for S3 and HDFS is deferred to Phase 2.
 *
 * <p>All write operations ({@link #newOutputFile}, {@link #deleteFile}) throw
 * {@link UnsupportedOperationException}; this class is intended for reading only.
 *
 * <p><b>Note:</b> Iceberg's internal {@code SeekableInputStream} (required by the Parquet
 * reader for footer-first access) is unrelated to Druid's {@code SeekableStream}, which
 * belongs to the Kafka Indexing Service.
 */
public class WarehouseFileIO implements FileIO
{
  @Override
  public InputFile newInputFile(String path)
  {
    String resolvedPath = stripFileScheme(path);
    if (isLocalPath(resolvedPath)) {
      return Files.localInput(resolvedPath);
    }
    throw new UnsupportedOperationException(
        "WarehouseFileIO does not support non-local paths in Phase 1. "
        + "Path: [" + path + "]. "
        + "Workers will fall back to catalog FileIO for S3 and HDFS paths."
    );
  }

  @Override
  public OutputFile newOutputFile(String path)
  {
    throw new UnsupportedOperationException("WarehouseFileIO is read-only");
  }

  @Override
  public void deleteFile(String path)
  {
    throw new UnsupportedOperationException("WarehouseFileIO is read-only");
  }

  private static String stripFileScheme(String path)
  {
    if (path.startsWith("file://")) {
      return path.substring(7);
    }
    return path;
  }

  private static boolean isLocalPath(String path)
  {
    // Local if it starts with a path separator (absolute path) or has no URI scheme at all
    return path.startsWith("/") || !path.contains("://");
  }
}
