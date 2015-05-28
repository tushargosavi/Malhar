package com.datatorrent.contrib.store;

import com.datatorrent.common.util.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DefaultKeyValStore implements KeyValStore
{
  private static final int MAX_L0_FILES = 4;
  TreeMap<Slice, Slice> memBuf = new TreeMap<Slice, Slice>();
  private int maxSize = 32 * 1024 * 1024;
  private int memUsed;
  String basePath;
  private FileSystem fs;
  private Index index;
  private transient ExecutorService executorService;
  private Comparator<Slice> comparator;
  private boolean level_o_compaction_active = false;

  public DefaultKeyValStore() {

  }

  void setup() throws IOException
  {
    Path baseDir = new Path(basePath);
    fs = FileSystem.newInstance(baseDir.toUri(), new Configuration());
    executorService = Executors.newScheduledThreadPool(10);
  }

  void close() throws IOException
  {
    if (fs != null)
      fs.close();
  }

  @Override public void put(Slice key, Slice value)
  {
    memBuf.put(key, value);
    memUsed = key.length + value.length;
    // TODO update log
  }

  @Override public Slice get(Slice key)
  {
    return null;
  }

  void commit() throws IOException
  {
    if (memUsed > maxSize) {
      flushData("0.time");
    }
  }

  private void flushData(String fileName) throws IOException
  {
    TFile.Writer writer = new TFile.Writer(fs.create(new Path(fileName)), 32 * 1024, null, null, new Configuration());
    Slice startKey = null;
    Slice endKey = null;

    int count = 0;
    for (Map.Entry<Slice, Slice> entry : memBuf.entrySet()) {
      if (startKey == null) startKey = entry.getKey();
      writer.append(entry.getKey().toByteArray(), entry.getValue().toByteArray());
      endKey = entry.getKey();
      count++;
    }
    writer.close();
    FileInfo finfo = new FileInfo(fileName, 0, count, startKey, endKey);
  }

  public void level0_add(FileInfo finfo) {
    LevelIndex linfo = index.get(0);
    linfo.addFile(finfo);
    if (linfo.numFiles() > MAX_L0_FILES) {
      start_level_0_compaction();
    }
  }

  synchronized private void start_level_0_compaction()
  {
    /* avoid running multiple level 0 compaction together */
    if (level_o_compaction_active)
      return;
    Runnable task = new Runnable() {
      @Override public void run()
      {
        try {
          level_0_compaction();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    executorService.submit(task);
  }

  private void level_0_compaction() throws IOException
  {
    FileInfo file1 = index.get(0).get(0);
    Slice start = file1.startKey;
    Slice end = file1.lastKey;

    /* find matching files in level n */
    List<FileInfo> list = getFilesInRange(1, start, end);
    List<FileInfo> lst1 = new ArrayList<FileInfo>();
    lst1.add(file1);
    // TODO get list of files from level 0 which might be having key range in same
    // and merge them and them compact with level 1 files.
    compactFiles(lst1, list);
  }

  /* crud compaction, read all files in memory and then keep writing,
   * return list of new files written */
  /* lst1 one contains file from level n
     lst2 contains files from level n+1 which overlaps with the given range.
   */
  private List<FileInfo> compactFiles(List<FileInfo> lst1, List<FileInfo> lst2) throws IOException
  {
    TFile.Reader.Scanner s1 = createScanner(lst1.get(0));
    TFile.Writer writer = new TFile.Writer(fs.create(new Path("lv1.out")), 32 * 1024, null, null, new Configuration());

    // do a merge between files
    Iterator<FileInfo> iter = lst2.iterator();
    while (iter.hasNext()) {
      FileInfo finfo = iter.next();
      TFile.Reader.Scanner s2 = createScanner(finfo);
      while (!s1.atEnd() && !s2.atEnd()) {
        TFile.Reader.Scanner.Entry e1 = s1.entry();
        TFile.Reader.Scanner.Entry e2 = s2.entry();
        if (compareEntries(e1, e2) <= 0) {
          writer.append(getKey(e1), getVal(e1));
          s1.advance();
        } else {
          writer.append(getKey(e2), getVal(e2));
          s2.advance();
          if (s2.atEnd()) {
            finfo = iter.next();
            s2 = createScanner(finfo);
            s2.advance();
          }
        }
      }
    }
    return null;
  }

  private int compareEntries(TFile.Reader.Scanner.Entry e1, TFile.Reader.Scanner.Entry e2) throws IOException
  {
    byte[] otherKey = getKey(e2);
    return e1.compareTo(otherKey);
  }

  private byte[] getVal(TFile.Reader.Scanner.Entry e) throws IOException
  {
    int valLen = e.getValueLength();
    byte[] val = new byte[valLen];
    e.getKey(val);
    return val;
  }

  private byte[] getKey(TFile.Reader.Scanner.Entry e) throws IOException
  {
    int keyLen = e.getKeyLength();
    byte[] key = new byte[keyLen];
    e.getKey(key);
    return key;
  }

  /* Returns list of file which conatains key ranges between start and end
   * from level lvl */
  List<FileInfo> getFilesInRange(int lvl, Slice start, Slice end) {
    List<FileInfo> includeFiles = new ArrayList<FileInfo>();
    LevelIndex lidx = index.get(lvl);
    for(FileInfo file : lidx.files) {
      if (comparator.compare(file.lastKey, start) >= 0 &&
          (comparator.compare(file.startKey, end) <= 0)) {
        includeFiles.add(file);
      }
    }
    return includeFiles;
  }

  TFile.Reader.Scanner createScanner(FileInfo finfo) throws IOException
  {
    FSDataInputStream di = fs.open(new Path(finfo.path));
    TFile.Reader reader = new TFile.Reader(di, finfo.size, new Configuration());
    TFile.Reader.Scanner scanner = reader.createScanner();
    return scanner;
  }

  public void flush()
  {

  }
}
