package io.druid.segment.data;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.metamx.common.IAE;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

public abstract class CompressionFactory
{
  public enum CompressionFormat
  {
    LZF((byte) 0x0) {
      @Override
      public Supplier<IndexedLongs> getLongsSupplier(int totalSize, int sizePer, ByteBuffer buffer, ByteOrder order)
      {
        return new BlockCompressionFormatSerde.BlockCompressedIndexedLongsSupplier(
            totalSize, sizePer, buffer, order, CompressedObjectStrategy.CompressionStrategy.LZF);
      }
    },

    LZ4((byte) 0x1) {
      @Override
      public Supplier<IndexedLongs> getLongsSupplier(int totalSize, int sizePer, ByteBuffer buffer, ByteOrder order)
      {
        return new BlockCompressionFormatSerde.BlockCompressedIndexedLongsSupplier(
            totalSize, sizePer, buffer, order, CompressedObjectStrategy.CompressionStrategy.LZ4);
      }
    },

    DELTA((byte) 0x2) {
      @Override
      public Supplier<IndexedLongs> getLongsSupplier(int totalSize, int sizePer, ByteBuffer buffer, ByteOrder order)
      {
        return new DeltaCompressionFormatSerde.DeltaCompressedIndexedLongsSupplier(totalSize, buffer, order);
      }
    },

    TABLE((byte) 0x3) {
      @Override
      public Supplier<IndexedLongs> getLongsSupplier(int totalSize, int sizePer, ByteBuffer buffer, ByteOrder order)
      {
        return new TableCompressionFormatSerde.TableCompressedIndexedLongsSupplier(totalSize, buffer, order);
      }
    },

    UNCOMPRESSED((byte) 0xFE) {
      @Override
      public Supplier<IndexedLongs> getLongsSupplier(int totalSize, int sizePer, ByteBuffer buffer, ByteOrder order)
      {
        return new UncompressedFormatSerde.UncompressedIndexedLongsSupplier(totalSize, buffer, order);
      }
    },

    UNCOMPRESSED_BLOCK((byte) 0xFF) {
      @Override
      public Supplier<IndexedLongs> getLongsSupplier(int totalSize, int sizePer, ByteBuffer buffer, ByteOrder order)
      {
        return new BlockUncompressedFormatSerde.BlockUncompressedIndexedLongsSupplier(totalSize, sizePer, buffer, order);
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
  }
}
