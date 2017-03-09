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

import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.spatial.ImmutableRTree;
import io.druid.collections.spatial.RTree;
import io.druid.collections.spatial.split.LinearGutmanSplitStrategy;
import io.druid.common.utils.SerializerUtils;
import io.druid.java.util.common.io.Closer;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferWriter;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.IndexedRTree;
import io.druid.segment.data.VSizeIndexedWriter;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.File;
import java.io.IOException;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class StringDimensionMergerLegacy extends StringDimensionMergerV9 implements DimensionMergerLegacy<int[]>
{
  private static final Logger log = new Logger(StringDimensionMergerLegacy.class);

  private VSizeIndexedWriter encodedValueWriterV8;

  public StringDimensionMergerLegacy(
      String dimensionName,
      IndexSpec indexSpec,
      File outDir,
      ColumnCapabilities capabilities,
      ProgressIndicator progress
  )
  {
    super(dimensionName, indexSpec, outDir, capabilities, progress);
  }

  @Override
  protected void setupEncodedValueWriter() throws IOException
  {
    encodedValueWriterV8 = new VSizeIndexedWriter(cardinality);
    encodedValueWriterV8.open();
  }

  @Override
  protected void processMergedRowHelper(int[] vals) throws IOException
  {
    IntList listToWrite = (vals == null) ? null : IntArrayList.wrap(vals);
    encodedValueWriterV8.add(listToWrite);
  }

  @Override
  public void writeIndexes(List<IntBuffer> segmentRowNumConversions, Closer closer) throws IOException
  {
    long dimStartTime = System.currentTimeMillis();
    final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();

    String bmpFilename = String.format("%s.inverted", dimensionName);
    bitmapWriter = new GenericIndexedWriter<>(bmpFilename, bitmapSerdeFactory.getObjectStrategy());
    bitmapWriter.open();
    final BitmapFactory bitmapFactory = bitmapSerdeFactory.getBitmapFactory();

    RTree tree = null;
    spatialWriter = null;
    boolean hasSpatial = capabilities.hasSpatialIndexes();
    if (hasSpatial) {
      spatialWriter = new ByteBufferWriter<>(new IndexedRTree.ImmutableRTreeObjectStrategy(bitmapFactory));
      spatialWriter.open();
      tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bitmapFactory), bitmapFactory);
    }

    IndexSeeker[] dictIdSeeker = toIndexSeekers(adapters, dimConversions, dimensionName);

    //Iterate all dim values's dictionary id in ascending order which in line with dim values's compare result.
    for (int dictId = 0; dictId < dictionarySize; dictId++) {
      progress.progress();
      mergeBitmaps(
          segmentRowNumConversions,
          bitmapFactory,
          tree,
          hasSpatial,
          dictIdSeeker,
          dictId
      );
    }

    log.info("Completed dimension[%s] in %,d millis.", dimensionName, System.currentTimeMillis() - dimStartTime);

    if (hasSpatial) {
      spatialWriter.write(ImmutableRTree.newImmutableFromMutable(tree));

    }
  }

  @Override
  public void writeValueMetadataToFile(final File valueEncodingFile) throws IOException
  {
    final SerializerUtils serializerUtils = new SerializerUtils();

    try (FileChannel out = open(valueEncodingFile)) {
      serializerUtils.writeString(out, dimensionName);
      dictionaryWriter.writeTo(out, null);
    }
  }

  private static FileChannel open(File file) throws IOException
  {
    return FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
  }

  @Override
  public void writeRowValuesToFile(File rowValueFile) throws IOException
  {
    try (FileChannel out = open(rowValueFile)) {
      encodedValueWriterV8.writeTo(out, null);
    }
  }

  @Override
  public void writeIndexesToFiles(final File invertedIndexFile, final File spatialIndexFile) throws IOException
  {
    final SerializerUtils serializerUtils = new SerializerUtils();

    try (FileChannel invertedIndexOut = open(invertedIndexFile)) {
      serializerUtils.writeString(invertedIndexOut, dimensionName);
      bitmapWriter.writeTo(invertedIndexOut, null);
    }

    if (capabilities.hasSpatialIndexes()) {
      try (FileChannel spatialIndexOut = open(spatialIndexFile)) {
        serializerUtils.writeString(spatialIndexOut, dimensionName);
        spatialWriter.writeTo(spatialIndexOut, null);
      }
    }
  }

  @Override
  public File makeDimFile() throws IOException
  {
    return IndexIO.makeDimFile(outDir, dimensionName);
  }
}


