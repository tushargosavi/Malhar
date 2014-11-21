package com.datatorrent.demos.dimensions.benchmark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFSWriteTest
{
  public static void main(String[] args)
  {
    String basePath = args[0];
    Path dataFilePath = new Path(basePath);
    try {
      FileSystem fs = FileSystem.newInstance(dataFilePath.toUri(), new Configuration());
      FSDataOutputStream out = fs.create(dataFilePath);
      byte[] buffer = new byte[1024 * 1024];
      for(int i = 0; i < buffer.length; i++) {
        buffer[i] = (byte)i;
      }

      long count = 0;
      long start = System.currentTimeMillis();
      while (true) {
        out.write(buffer);
        count++;
        out.hflush();
        long elapse = System.currentTimeMillis() - start;
        if (elapse > 60 * 1000)
          break;
      }
      System.out.println("Wrote " + count + "MB of data in one minute");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

