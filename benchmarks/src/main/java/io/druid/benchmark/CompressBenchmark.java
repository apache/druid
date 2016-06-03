package io.druid.benchmark;

import com.google.common.io.Files;
import com.google.common.io.OutputSupplier;
import io.druid.collections.ResourceHolder;
import io.druid.segment.data.CompressedLongBufferObjectStrategy;
import io.druid.segment.data.CompressedLongsIndexedSupplier;
import io.druid.segment.data.CompressedLongsSupplierSerializer;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.IndexedLongs;
import io.druid.segment.data.TmpFileIOPeon;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;

@State(Scope.Benchmark)
public class CompressBenchmark
{
  @Param({"reqInt", "bytesLong", "timestamp", "added", "delta"})
  static String arg;

//  @Param({"1024", "8192", "131072"})
  @Param(CompressedLongsIndexedSupplier.MAX_LONGS_IN_BUFFER + "")
  static int sizePer;

  final static File baseDir = new File("comptest");

  @State(Scope.Benchmark)
  public static class DruidReadState {
    ByteBuffer buffer;
    CompressedLongsIndexedSupplier supplier;
    Random rand;
    IndexedLongs indexedLongs;
    int count;
    long sum;
    @Setup
    public void setup() throws IOException
    {
      buffer = Files.map(new File(baseDir, arg + ".druid"));
      supplier =  CompressedLongsIndexedSupplier.fromByteBuffer(buffer, ByteOrder.nativeOrder());
      rand = new Random();
      indexedLongs = supplier.get();
      count = indexedLongs.size();
      System.out.println("count : " + count);
    }

    @TearDown
    public void tearDown()
    {
      System.out.println("sum : " + sum);
    }
  }

  @State(Scope.Benchmark)
  public static class LZ4DruidReadState {
    ByteBuffer buffer;
    CompressedLongsIndexedSupplier supplier;
    Random rand;
    IndexedLongs indexedLongs;
    int count;
    long sum;
    @Setup
    public void setup() throws IOException
    {
      buffer = Files.map(new File(baseDir, arg + ".druidlz4" + sizePer));
      supplier =  CompressedLongsIndexedSupplier.fromByteBuffer(buffer, ByteOrder.nativeOrder());
      rand = new Random();
      indexedLongs = supplier.get();
      count = indexedLongs.size();
      System.out.println("count : " + count);
    }

    @TearDown
    public void tearDown()
    {
      System.out.println("sum : " + sum);
    }
  }

  @State(Scope.Benchmark)
  public static class LuceneReadState {
    MMapDirectory dir;
    Random rand;
    DirectoryReader reader;
    LeafReader leafReader;
    NumericDocValues docVals;
    int maxDoc;
    long sum;
    @Setup
    public void setup() throws IOException
    {
      dir = new MMapDirectory((new File(baseDir, arg + "lucene")).toPath());
      rand = new Random();
      reader = DirectoryReader.open(dir);
      leafReader = reader.leaves().get(0).reader();
      docVals = leafReader.getNumericDocValues("num");
      maxDoc = leafReader.maxDoc();
      System.out.println("maxDoc : " + maxDoc);
    }

    @TearDown
    public void tearDown()
    {
      System.out.println("sum : " + sum);
    }
  }

//  @Benchmark
//  public void buildDruid() throws Exception
//  {
//    File inFile = new File(baseDir, arg + ".out");
//    File outFile = new File(baseDir, arg + ".druid" + sizePer);
//    outFile.delete();
//    TmpFileIOPeon iopeon = new TmpFileIOPeon(true);
//    CompressedLongsSupplierSerializer writer = new CompressedLongsSupplierSerializer(
//        sizePer,
//        new GenericIndexedWriter<ResourceHolder<LongBuffer>>(
//            iopeon,
//            "long",
//            CompressedLongBufferObjectStrategy.getBufferForOrder(
//                ByteOrder.nativeOrder(),
//                CompressedObjectStrategy.CompressionStrategy.LZ4,
//                CompressedLongsIndexedSupplier.MAX_LONGS_IN_BUFFER
//            )
//        ),
//        CompressedObjectStrategy.CompressionStrategy.LZ4
//    );
//
//
//    writer.open();
//    LineIterator it = FileUtils.lineIterator(inFile, "UTF-8");
//
//    try (FileChannel output = FileChannel.open(outFile.toPath(),
//                                               StandardOpenOption.CREATE_NEW,
//                                               StandardOpenOption.WRITE)){
//      writer.open();
//      while (it.hasNext()) {
//        String line = it.nextLine();
//        long val = Long.parseLong(line);
//        //writer.serialize(val);
//        writer.add(val);
//      }
//      System.out.println(writer.getSerializedSize());
//
//      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
//      writer.closeAndConsolidate(
//          new OutputSupplier<OutputStream>()
//          {
//            @Override
//            public OutputStream getOutput() throws IOException
//            {
//              return baos;
//            }
//          }
//      );
//      output.write(ByteBuffer.wrap(baos.toByteArray()));
//    } finally {
//      iopeon.cleanup();
//      it.close();
//    }
//    long outSize = outFile.length();
//    long inSize = inFile.length();
//    System.out.println("Size : " + outSize + ", factor : " + (double)outSize/inSize);
//  }
//
//  @Benchmark
//  public void buildLucene() throws Exception
//  {
//    File input = new File(baseDir, arg + ".out");
//    File output = new File(baseDir, arg + "lucene");
//    FileUtils.deleteDirectory(output);
//    Directory dir = FSDirectory.open(output.toPath());
//    IndexWriterConfig writerConf = new IndexWriterConfig(null);
//    IndexWriter writer = new IndexWriter(dir, writerConf);
//    LineIterator it = FileUtils.lineIterator(input, "UTF-8");
//    try {
//      while (it.hasNext()) {
//        String line = it.nextLine();
//        long val = Long.parseLong(line);
//        Document doc = new Document();
//        doc.add(new NumericDocValuesField("num", val));
//        writer.addDocument(doc);
//      }
//      writer.forceMerge(1);
//      writer.commit();
//    } finally {
//      it.close();
//      writer.close();
//    }
//    long outSize = FileUtils.sizeOfDirectory(output);
//    long inSize = input.length();
//    System.out.println("Size : " + outSize + ", factor : " + (double)outSize/inSize);
//  }

  @Benchmark
  public void readDruidUncompressed(DruidReadState state) throws Exception
  {
    state.sum = 0;
    for (int i = 0; i < state.count; i++) {
      state.sum += state.indexedLongs.get(i);
    }
  }

  @Benchmark
  public void readDruidLz4(LZ4DruidReadState state) throws Exception
  {
    state.sum = 0;
    for (int i = 0; i < state.count; i++/*i+= state.rand.nextInt(10000)*/) {
      state.sum += state.indexedLongs.get(i);
    }
  }

  @Benchmark
  public void readLucene(LuceneReadState state) throws Exception
  {
    state.sum = 0;
    for (int i = 0; i < state.maxDoc; i++) {
      state.sum += state.docVals.get(i);
    }
  }

  public static void main(String[] args) throws Exception
  {
    Options opt = new OptionsBuilder()
        .include(CompressBenchmark.class.getSimpleName())
        .forks(1)
        .warmupForks(0)
        .warmupIterations(1)
        .measurementIterations(3)
        .mode(Mode.AverageTime)
        .timeUnit(TimeUnit.MILLISECONDS)
        .build();
    new Runner(opt).run();

//    ByteBuffer buffer;
//    CompressedLongsIndexedSupplier supplier;
//    Random rand;
//    IndexedLongs indexedLongs;
//    int count;
//    long sum;
//    buffer = Files.map(new File(baseDir, "reqInt.druidlz48192"));
//    supplier =  CompressedLongsIndexedSupplier.fromByteBuffer(buffer, ByteOrder.nativeOrder());
//    rand = new Random();
//    indexedLongs = supplier.get();
//    count = indexedLongs.size();
//    System.out.println("count : " + count);
//    System.out.println("buffer : " + buffer.limit() + " " + buffer.capacity() + " " + buffer.position() + " " + buffer.remaining());
//    System.out.println(indexedLongs.get(2100000));



//    MMapDirectory dir;
//    Random rand;
//    DirectoryReader reader;
//    LeafReader leafReader;
//    NumericDocValues docVals;
//    int maxDoc;
//      dir = new MMapDirectory((new File(baseDir, "reqIntlucene")).toPath());
//      rand = new Random();
//      reader = DirectoryReader.open(dir);
//      leafReader = reader.leaves().get(0).reader();
//      docVals = leafReader.getNumericDocValues("num");
//      maxDoc = leafReader.maxDoc();
//      System.out.println("maxDoc : " + maxDoc);
//    System.out.println(docVals.get(2100000));
//    while (true) {
//
//    }
  }

}

