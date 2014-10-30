package com.datatorrent.demos.dimensions.ads;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

public class DimensionsOrcFile
{
  public static final String MAGIC = "ORC";
  public static final String COMPRESSION = "orc.compress";
  public static final String COMPRESSION_BLOCK_SIZE = "orc.compress.size";
  public static final String STRIPE_SIZE = "orc.stripe.size";
  public static final String ROW_INDEX_STRIDE = "orc.row.index.stride";
  public static final String ENABLE_INDEXES = "orc.create.index";

  public static class KeyWrapper implements WritableComparable<KeyWrapper> {
    public StripeInformation key;
    public Path inputPath;
    public ObjectInspector objectInspector;
    public CompressionKind compression;
    public int compressionSize;
    public int rowIndexStride;
    public ColumnStatistics[] columnStats;
    public Map<String, ByteBuffer> userMetadata;

    @Override
    public void readFields(DataInput in) throws IOException {
      throw new RuntimeException("Not supported.");

    }
    @Override
    public void write(DataOutput out) throws IOException {
      throw new RuntimeException("Not supported.");

    }
    @Override
    public int compareTo(KeyWrapper o) {
      throw new RuntimeException("Not supported.");
    }

    @Override
    public void write(DataOutput d) throws IOException
    {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void readFields(DataInput di) throws IOException
    {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }

  public static class ValueWrapper implements WritableComparable<ValueWrapper> {
    public byte[] value;

    @Override
    public void readFields(DataInput in) throws IOException {
      throw new RuntimeException("Not supported.");
    }

    @Override
    public void write(DataOutput out) throws IOException {
      throw new RuntimeException("Not supported.");
    }

    @Override
    public int compareTo(ValueWrapper o) {
      throw new RuntimeException("Not supported.");
    }
  }

  /**
   * Create an ORC file reader.
   * @param fs file system
   * @param path file name to read from
   * @return a new ORC file reader.
   * @throws IOException
   */
  public static Reader createReader(FileSystem fs, Path path, Configuration conf )
      throws IOException {
    return new ReaderImpl(fs, path, conf);
  }

  /**
   * Create an ORC file streamFactory.
   * @param fs file system
   * @param path filename to write to
   * @param inspector the ObjectInspector that inspects the rows
   * @param stripeSize the number of bytes in a stripe
   * @param compress how to compress the file
   * @param bufferSize the number of bytes to compress at once
   * @param rowIndexStride the number of rows between row index entries or
   *                       0 to suppress all indexes
   * @return a new ORC file streamFactory
   * @throws IOException
   */
  public static Writer createWriter(FileSystem fs,
                                    Path path,
                                    Configuration conf,
                                    ObjectInspector inspector,
                                    long stripeSize,
                                    CompressionKind compress,
                                    int bufferSize,
                                    int rowIndexStride) throws IOException {
    return new WriterImpl(fs, path, conf, inspector, stripeSize, compress,
      bufferSize, rowIndexStride, getMemoryManager(conf));
  }

  private static MemoryManager memoryManager = null;

  private static synchronized MemoryManager getMemoryManager(
      Configuration conf) {

    if (memoryManager == null) {
      memoryManager = new MemoryManager(conf);
    }
    return memoryManager;
  }
}

}
