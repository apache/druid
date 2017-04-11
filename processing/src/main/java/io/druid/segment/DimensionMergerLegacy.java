/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import com.google.common.io.ByteSink;
import com.google.common.io.OutputSupplier;
import io.druid.common.guava.FileOutputSupplier;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Processing related interface
 *
 * DimensionMerger subclass to be used with the legacy IndexMerger.
 *
 * NOTE: Remove this class when the legacy IndexMerger is deprecated and removed.
 */
public interface DimensionMergerLegacy<EncodedKeyComponentType> extends DimensionMergerV9<EncodedKeyComponentType>
{
  /**
   * Write this dimension's value metadata to a file.
   *
   * @param valueEncodingFile Destination file
   * @throws IOException
   */
  void writeValueMetadataToFile(FileOutputSupplier valueEncodingFile) throws IOException;


  /**
   * Write this dimension's sequence of row values to a file.
   * @param rowValueOut Destination file
   * @throws IOException
   */
  void writeRowValuesToFile(FileOutputSupplier rowValueOut) throws IOException;


  /**
   * Write this dimension's bitmap and spatial indexes to a file.
   * @param invertedOut Destination file for bitmap indexes
   * @param spatialOut Destination file for spatial indexes
   * @throws IOException
   */
  void writeIndexesToFiles(
      ByteSink invertedOut,
      OutputSupplier<FileOutputStream> spatialOut
  ) throws IOException;


  File makeDimFile() throws IOException;
}
