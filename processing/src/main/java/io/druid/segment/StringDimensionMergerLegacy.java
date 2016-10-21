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
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.google.common.io.OutputSupplier;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.collections.spatial.RTree;
import com.metamx.collections.spatial.split.LinearGutmanSplitStrategy;
import io.druid.common.guava.FileOutputSupplier;
import io.druid.common.utils.SerializerUtils;
import io.druid.java.util.common.ByteBufferUtils;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferWriter;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedRTree;
import io.druid.segment.data.TmpFileIOPeon;
import io.druid.segment.data.VSizeIndexedWriter;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.util.List;

public class StringDimensionMergerLegacy extends StringDimensionMergerV9 implements DimensionMergerLegacy<int[]>
{
  private static final Logger log = new Logger(StringDimensionMergerLegacy.class);

  private VSizeIndexedWriter encodedValueWriterV8;
  private IOPeon spatialIoPeon;
  private File dictionaryFile;

  public StringDimensionMergerLegacy(
      String dimensionName,
      IndexSpec indexSpec,
      File outDir,
      IOPeon ioPeon,
      ColumnCapabilities capabilities,
      ProgressIndicator progress
  )
  {
    super(dimensionName, indexSpec, outDir, ioPeon, capabilities, progress);
  }

  @Override
  protected void setupEncodedValueWriter() throws IOException
  {
    encodedValueWriterV8 = new VSizeIndexedWriter(ioPeon, dimensionName, cardinality);
    encodedValueWriterV8.open();
  }

  @Override
  protected void processMergedRowHelper(int[] vals) throws IOException
  {
    List<Integer> listToWrite = (vals == null)
                                ? null
                                : Ints.asList(vals);
    encodedValueWriterV8.add(listToWrite);
  }

  @Override
  public void writeIndexes(List<IntBuffer> segmentRowNumConversions, Closer closer) throws IOException
  {
    final SerializerUtils serializerUtils = new SerializerUtils();
    long dimStartTime = System.currentTimeMillis();
    final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();

    String bmpFilename = String.format("%s.inverted", dimensionName);
    bitmapWriter = new GenericIndexedWriter<>(
        ioPeon,
        bmpFilename,
        bitmapSerdeFactory.getObjectStrategy()
    );
    bitmapWriter.open();

    final MappedByteBuffer dimValsMapped = Files.map(dictionaryFile);
    closer.register(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        ByteBufferUtils.unmap(dimValsMapped);
      }
    });

    if (!dimensionName.equals(serializerUtils.readString(dimValsMapped))) {
      throw new ISE("dimensions[%s] didn't equate!?  This is a major WTF moment.", dimensionName);
    }
    Indexed<String> dimVals = GenericIndexed.read(dimValsMapped, GenericIndexed.STRING_STRATEGY);
    log.info("Starting dimension[%s] with cardinality[%,d]", dimensionName, dimVals.size());


    final BitmapFactory bmpFactory = bitmapSerdeFactory.getBitmapFactory();

    RTree tree = null;
    spatialWriter = null;
    boolean hasSpatial = capabilities.hasSpatialIndexes();
    spatialIoPeon = new TmpFileIOPeon();
    if (hasSpatial) {
      String spatialFilename = String.format("%s.spatial", dimensionName);
      spatialWriter = new ByteBufferWriter<>(
          spatialIoPeon, spatialFilename, new IndexedRTree.ImmutableRTreeObjectStrategy(bmpFactory)
      );
      spatialWriter.open();
      tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bmpFactory), bmpFactory);
    }

    IndexSeeker[] dictIdSeeker = toIndexSeekers(adapters, dimConversions, dimensionName);

    //Iterate all dim values's dictionary id in ascending order which in line with dim values's compare result.
    for (int dictId = 0; dictId < dimVals.size(); dictId++) {
      progress.progress();
      mergeBitmaps(
          segmentRowNumConversions,
          dimVals,
          bmpFactory,
          tree,
          hasSpatial,
          dictIdSeeker,
          dictId,
          adapters,
          dimensionName,
          nullRowsBitmap,
          bitmapWriter
      );
    }

    log.info("Completed dimension[%s] in %,d millis.", dimensionName, System.currentTimeMillis() - dimStartTime);

    if (hasSpatial) {
      spatialWriter.write(ImmutableRTree.newImmutableFromMutable(tree));

    }
  }

  @Override
  public void writeValueMetadataToFile(final FileOutputSupplier valueEncodingFile) throws IOException
  {
    final SerializerUtils serializerUtils = new SerializerUtils();

    dictionaryWriter.close();
    serializerUtils.writeString(valueEncodingFile, dimensionName);
    ByteStreams.copy(dictionaryWriter.combineStreams(), valueEncodingFile);

    // save this File reference, we will read from it later when building bitmap/spatial indexes
    dictionaryFile = valueEncodingFile.getFile();
  }

  @Override
  public void writeRowValuesToFile(FileOutputSupplier rowValueFile) throws IOException
  {
    encodedValueWriterV8.close();
    ByteStreams.copy(encodedValueWriterV8.combineStreams(), rowValueFile);
  }

  @Override
  public void writeIndexesToFiles(
      final ByteSink invertedIndexFile,
      final OutputSupplier<FileOutputStream> spatialIndexFile
  ) throws IOException
  {
    final SerializerUtils serializerUtils = new SerializerUtils();
    final OutputSupplier<OutputStream> invertedIndexOutputSupplier = new OutputSupplier<OutputStream>()
    {
      @Override
      public OutputStream getOutput() throws IOException
      {
        return invertedIndexFile.openStream();
      }
    };

    bitmapWriter.close();
    serializerUtils.writeString(invertedIndexOutputSupplier, dimensionName);
    ByteStreams.copy(bitmapWriter.combineStreams(), invertedIndexOutputSupplier);


    if (capabilities.hasSpatialIndexes()) {
      spatialWriter.close();
      serializerUtils.writeString(spatialIndexFile, dimensionName);
      ByteStreams.copy(spatialWriter.combineStreams(), spatialIndexFile);
      spatialIoPeon.cleanup();
    }
  }
}
