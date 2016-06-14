package io.druid.benchmark;

import com.google.common.io.ByteSink;
import com.google.common.io.Files;
import io.druid.segment.data.CompressedLongsIndexedSupplier;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.IndexedLongs;
import io.druid.segment.data.LongSupplierSerializer;
import io.druid.segment.data.TmpFileIOPeon;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class CompressionFormatBenchmark
{
  @Param({"reqInt", "bytesLong", "timestamp"})
  private static String file;

  @Param({"lz4", "uncompressed", "uncompressed_new", "delta"})
  private static String format;

  private File compFile;
  private ByteBuffer buffer;
  private Random rand;
  private IndexedLongs indexedLongs;
  private int count;
  private long sum;

  @Setup
  public void setup() throws Exception
  {
    buildDruid();
    rand = new Random();
    buffer = Files.map(compFile);
    indexedLongs = CompressedLongsIndexedSupplier.fromByteBuffer(buffer, ByteOrder.nativeOrder()).get();
    count = indexedLongs.size();
    System.out.println("count : " + count);
  }

  @TearDown
  public void teardown()
  {
    System.out.println("sum : " + sum);
  }

  private void buildDruid() throws IOException, URISyntaxException
  {
    URL url = this.getClass().getClassLoader().getResource(file + ".gz");
    File inFile = new File(url.toURI());
    compFile = new File(inFile.getParent(), file + "." + format);
    CompressionFactory.CompressionFormat compressionFormat = CompressionFactory.CompressionFormat.valueOf(format.toUpperCase());
    compFile.delete();
    TmpFileIOPeon iopeon = new TmpFileIOPeon(true);
    LongSupplierSerializer writer = compressionFormat.getLongSerializer(
        iopeon,
        "long",
        ByteOrder.nativeOrder()
    );
    GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(inFile));
    BufferedReader br = new BufferedReader(new InputStreamReader(gzip));

    try (FileChannel output = FileChannel.open(compFile.toPath(),
                                               StandardOpenOption.CREATE_NEW,
                                               StandardOpenOption.WRITE)){
      writer.open();
      String line;
      while ((line = br.readLine()) != null) {
        writer.add(Long.parseLong(line));
      }
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      writer.closeAndConsolidate(
          new ByteSink()
          {
            @Override
            public OutputStream openStream() throws IOException
            {
              return baos;
            }
          }
      );
      output.write(ByteBuffer.wrap(baos.toByteArray()));
    } finally {
      iopeon.cleanup();
      br.close();
    }
  }

  @Benchmark
  public void readContinuous() {
    sum = 0;
    for (int i = 0; i < count; i++) {
      sum += indexedLongs.get(i);
    }
  }

  @Benchmark
  public void readSkipping() {
    sum = 0;
    for (int i = 0; i < count; i += rand.nextInt(10000)) {
      sum += indexedLongs.get(i);
    }
  }

}

