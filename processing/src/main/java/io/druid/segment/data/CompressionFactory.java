package io.druid.segment.data;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

public abstract class CompressionFactory
{

  //TODO decide on default compression format
  public static final CompressionFormat DEFAULT_COMPRESSION_FORMAT = CompressionFormat.LZ4;

  public enum CompressionFormat
  {
    LZF((byte) 0x0) {
      @Override
      public Supplier<IndexedLongs> getLongsSupplier(int totalSize, int sizePer, ByteBuffer buffer, ByteOrder order)
      {
        return new BlockCompressionFormatSerde.BlockCompressedIndexedLongsSupplier(
            totalSize, sizePer, buffer, order, CompressedObjectStrategy.CompressionStrategy.LZF);
      }

      @Override
      public LongSupplierSerializer getLongSerializer(IOPeon ioPeon, String filenameBase, ByteOrder order)
      {
        return new BlockCompressionFormatSerde.BlockCompressedLongSupplierSerializer(
            ioPeon, filenameBase, order, CompressedObjectStrategy.CompressionStrategy.LZF);
      }
    },

    LZ4((byte) 0x1) {
      @Override
      public Supplier<IndexedLongs> getLongsSupplier(int totalSize, int sizePer, ByteBuffer buffer, ByteOrder order)
      {
        return new BlockCompressionFormatSerde.BlockCompressedIndexedLongsSupplier(
            totalSize, sizePer, buffer, order, CompressedObjectStrategy.CompressionStrategy.LZ4);
      }

      @Override
      public LongSupplierSerializer getLongSerializer(IOPeon ioPeon, String filenameBase, ByteOrder order)
      {
        return new BlockCompressionFormatSerde.BlockCompressedLongSupplierSerializer(
            ioPeon, filenameBase, order, CompressedObjectStrategy.CompressionStrategy.LZ4);
      }
    },

    DELTA((byte) 0x2) {
      @Override
      public Supplier<IndexedLongs> getLongsSupplier(int totalSize, int sizePer, ByteBuffer buffer, ByteOrder order)
      {
        return new DeltaCompressionFormatSerde.DeltaCompressedIndexedLongsSupplier(totalSize, buffer, order);
      }

      @Override
      public LongSupplierSerializer getLongSerializer(IOPeon ioPeon, String filenameBase, ByteOrder order)
      {
        return new RACompressionFormatSerde.RACompressedLongSupplierSerializer(ioPeon, filenameBase, order);
      }
    },

    TABLE((byte) 0x3) {
      @Override
      public Supplier<IndexedLongs> getLongsSupplier(int totalSize, int sizePer, ByteBuffer buffer, ByteOrder order)
      {
        return new TableCompressionFormatSerde.TableCompressedIndexedLongsSupplier(totalSize, buffer, order);
      }

      @Override
      public LongSupplierSerializer getLongSerializer(IOPeon ioPeon, String filenameBase, ByteOrder order)
      {
        return new RACompressionFormatSerde.RACompressedLongSupplierSerializer(ioPeon, filenameBase, order);
      }
    },

    UNCOMPRESSED_NEW((byte) 0xFE) {
      @Override
      public Supplier<IndexedLongs> getLongsSupplier(int totalSize, int sizePer, ByteBuffer buffer, ByteOrder order)
      {
        return new UncompressedFormatSerde.UncompressedIndexedLongsSupplier(totalSize, buffer, order);
      }

      @Override
      public LongSupplierSerializer getLongSerializer(IOPeon ioPeon, String filenameBase, ByteOrder order)
      {
        return new UncompressedFormatSerde.UncompressedLongSupplierSerializer(ioPeon, filenameBase, order);
      }
    },

    UNCOMPRESSED((byte) 0xFF) {
      @Override
      public Supplier<IndexedLongs> getLongsSupplier(int totalSize, int sizePer, ByteBuffer buffer, ByteOrder order)
      {
        return new BlockUncompressedFormatSerde.BlockUncompressedIndexedLongsSupplier(totalSize, sizePer, buffer, order);
      }

      @Override
      public LongSupplierSerializer getLongSerializer(IOPeon ioPeon, String filenameBase, ByteOrder order)
      {
        return new BlockCompressionFormatSerde.BlockCompressedLongSupplierSerializer(
            ioPeon, filenameBase, order, CompressedObjectStrategy.CompressionStrategy.UNCOMPRESSED);
      }
    };

    final byte id;

    CompressionFormat(byte id)
    {
      this.id = id;
    }

    public byte getId()
    {
      return id;
    }

    static final Map<Byte, CompressionFormat> idMap = Maps.newHashMap();

    static {
      for (CompressionFormat format : CompressionFormat.values()) {
        idMap.put(format.getId(), format);
      }
    }

    public static CompressionFormat forId(byte id)
    {
      return idMap.get(id);
    }

    public abstract Supplier<IndexedLongs> getLongsSupplier(int totalSize, int sizePer, ByteBuffer buffer, ByteOrder order);

    public abstract LongSupplierSerializer getLongSerializer(IOPeon ioPeon, String filenameBase, ByteOrder order);
  }
}
