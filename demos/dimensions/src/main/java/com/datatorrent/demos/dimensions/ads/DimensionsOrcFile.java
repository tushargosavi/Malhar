package com.datatorrent.demos.dimensions.ads;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile.Writer;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.WritableComparable;

/**
 * Contains factory methods to read or write ORC files.
 */
public class DimensionsOrcFile
{

  public static final String MAGIC = "ORC";
  public static final String COMPRESSION = "orc.compress";
  public static final String COMPRESSION_BLOCK_SIZE = "orc.compress.size";
  public static final String STRIPE_SIZE = "orc.stripe.size";
  public static final String ROW_INDEX_STRIDE = "orc.row.index.stride";
  public static final String ENABLE_INDEXES = "orc.create.index";

  public enum CompressionKind
  {
    NONE, ZLIB, SNAPPY, LZO
  }

  public interface ColumnStatistics
  {
    /**
     * Get the number of values in this column. It will differ from the number
     * of rows because of NULL values and repeated values.
     *
     * @return the number of values
     */
    long getNumberOfValues();

  }

  /**
 * Information about the stripes in an ORC file that is provided by the Reader.
 */
public interface StripeInformation {
  /**
   * Get the byte offset of the start of the stripe.
   * @return the bytes from the start of the file
   */
  long getOffset();

  /**
   * Get the length of the stripe's indexes.
   * @return the number of bytes in the index
   */
  long getIndexLength();

  /**
   * Get the length of the stripe's data.
   * @return the number of bytes in the stripe
   */
  long getDataLength();

  /**
   * Get the length of the stripe's tail section, which contains its index.
   * @return the number of bytes in the tail
   */
  long getFooterLength();

  /**
   * Get the number of rows in the stripe.
   * @return a count of the number of rows
   */
  long getNumberOfRows();

  /**
   * Get the raw size of the data in the stripe.
   * @return the number of bytes of raw data
   */
  long getRawDataSize();
}

  public static class KeyWrapper implements WritableComparable<KeyWrapper>
  {
    public StripeInformation key;
    public Path inputPath;
    public ObjectInspector objectInspector;
    public CompressionKind compression;
    public int compressionSize;
    public int rowIndexStride;
    public ColumnStatistics[] columnStats;
    public Map<String, ByteBuffer> userMetadata;

    @Override
    public void readFields(DataInput in) throws IOException
    {
      throw new RuntimeException("Not supported.");

    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      throw new RuntimeException("Not supported.");

    }

    @Override
    public int compareTo(KeyWrapper o)
    {
      throw new RuntimeException("Not supported.");
    }

  }

  public static class ValueWrapper implements WritableComparable<ValueWrapper>
  {
    public byte[] value;

    @Override
    public void readFields(DataInput in) throws IOException
    {
      throw new RuntimeException("Not supported.");
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      throw new RuntimeException("Not supported.");
    }

    @Override
    public int compareTo(ValueWrapper o)
    {
      throw new RuntimeException("Not supported.");
    }

  }


  /**
   * Create an ORC file reader.
   *
   * @param fs file system
   * @param path file name to read from
   * @return a new ORC file reader.
   * @throws IOException
   */
  public static Reader createReader(FileSystem fs, Path path, Configuration conf)
          throws IOException
  {
    return new ReaderImpl(fs, path, conf);
  }

  /**
   * Create an ORC file streamFactory.
   *
   * @param fs file system
   * @param path filename to write to
   * @param inspector the ObjectInspector that inspects the rows
   * @param stripeSize the number of bytes in a stripe
   * @param compress how to compress the file
   * @param bufferSize the number of bytes to compress at once
   * @param rowIndexStride the number of rows between row index entries or
   * 0 to suppress all indexes
   * @return a new ORC file streamFactory
   * @throws IOException

  public static Writer createWriter(FileSystem fs,
          Path path,
          Configuration conf,
          ObjectInspector inspector,
          long stripeSize,
          CompressionKind compress,
          int bufferSize,
          int rowIndexStride) throws IOException
  {
    return new WriterImpl(fs, path, conf, inspector, stripeSize, compress,
                          bufferSize, rowIndexStride, getMemoryManager(conf));
  }

  private static MemoryManager memoryManager = null;

  private static synchronized MemoryManager getMemoryManager(
          Configuration conf)
  {

    if (memoryManager == null) {
      memoryManager = new MemoryManager(conf);
    }
    return memoryManager;
  }*/

}
