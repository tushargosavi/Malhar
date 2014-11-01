package com.datatorrent.demos.dimensions.ads;

import java.io.File;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.*;
import org.junit.rules.TestName;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.BytesWritable;

public class TestOrcFile
{
  Path workDir = new Path(System.getProperty("test.tmp.dir", "target" + File.separator + "test" + File.separator + "tmp"));

  public static class SimpleStruct
  {
    BytesWritable bytes1;

    SimpleStruct(BytesWritable b1)
    {
      this.bytes1 = b1;
    }

  }

  Configuration conf;
  FileSystem fs;
  Path testFilePathObject;
  Path testFilePathBytes;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem() throws Exception
  {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePathObject = new Path(workDir, "TestOrcFile." + testCaseName.getMethodName() + ".orc");
    testFilePathBytes = new Path(workDir, "TestOrcByteFile." + testCaseName.getMethodName() + ".orc");
    System.out.println("testfilepath is" + testFilePathObject.toString());
    fs.delete(testFilePathObject, false);
  }

  private static BytesWritable bytes(int... items)
  {
    BytesWritable result = new BytesWritable();
    result.setSize(items.length);
    for (int i = 0; i < items.length; ++i) {
      result.getBytes()[i] = (byte)items[i];
    }
    return result;
  }

  private static ByteBuffer byteBuf(int... items)
  {
    ByteBuffer result = ByteBuffer.allocate(items.length);
    for (int item: items) {
      result.put((byte)item);
    }
    return result;
  }

  @Test
  public void test1() throws Exception
  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(AdInfo.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePathObject,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .bufferSize(10000));
    AdInfo adinfo = new AdInfo();
    adinfo.setAdUnit(12);
    adinfo.setAdvertiserId(40);
    adinfo.setClicks(20000);
    adinfo.setCost(24.50);
    adinfo.setImpressions(4000);
    adinfo.setRevenue(45.60);
    adinfo.setPublisherId(34);
    adinfo.setTimestamp(1414799L);
    System.out.println("adrow is" + testFilePathObject.toString());

    writer.addRow(adinfo);
    adinfo = new AdInfo();
    adinfo.setAdUnit(15);
    adinfo.setAdvertiserId(45);
    adinfo.setClicks(25000);
    adinfo.setCost(34.50);
    adinfo.setImpressions(6000);
    adinfo.setRevenue(60.60);
    adinfo.setPublisherId(67);
    adinfo.setTimestamp(141479L);
    writer.addRow(adinfo);
    writer.close();

    Reader reader = OrcFile.createReader(fs, testFilePathObject);
    StructObjectInspector readerInspector = (StructObjectInspector)reader
            .getObjectInspector();
    List<? extends StructField> fields = readerInspector
            .getAllStructFieldRefs();
    IntObjectInspector publisher = (IntObjectInspector)readerInspector.getStructFieldRef("publisherid").getFieldObjectInspector();
    IntObjectInspector advertiser = (IntObjectInspector)readerInspector
            .getStructFieldRef("advertiserid").getFieldObjectInspector();
    IntObjectInspector adUnit = (IntObjectInspector)readerInspector
            .getStructFieldRef("adunit").getFieldObjectInspector();
    LongObjectInspector timestamp = (LongObjectInspector)readerInspector
            .getStructFieldRef("timestamp").getFieldObjectInspector();
    DoubleObjectInspector cost = (DoubleObjectInspector)readerInspector.getStructFieldRef("cost").getFieldObjectInspector();
    DoubleObjectInspector revenue = (DoubleObjectInspector)readerInspector.getStructFieldRef("revenue").getFieldObjectInspector();
    LongObjectInspector impressions = (LongObjectInspector)readerInspector
            .getStructFieldRef("impressions").getFieldObjectInspector();
    LongObjectInspector clicks = (LongObjectInspector)readerInspector
            .getStructFieldRef("clicks").getFieldObjectInspector();

    RecordReader rows = reader.rows();
    Object row = rows.next(null);
    int publisherId = publisher.get((readerInspector.getStructFieldData(row, fields.get(0))));
    int advertiserId = advertiser.get(readerInspector.getStructFieldData(row, fields.get(1)));
    int adInt = adUnit.get(readerInspector.getStructFieldData(row, fields.get(2)));
    Long time = timestamp.get(readerInspector.getStructFieldData(row, fields.get(3)));
    double costField = cost.get(readerInspector.getStructFieldData(row, fields.get(4)));
    double revenueField = revenue.get(readerInspector.getStructFieldData(row, fields.get(5)));
    Long impressionsField = impressions.get(readerInspector.getStructFieldData(row, fields.get(6)));
    Long clicksField = clicks.get(readerInspector.getStructFieldData(row, fields.get(6)));
    System.out.println("row 1 is" + "publisher Id is " + publisherId + "advertiserId is " + advertiserId + "adInt is " + adInt + "time is" + time + "cost is" + costField + "revenue is" + revenueField + "impressions is" + impressionsField + "clicks is" + clicksField);


      row = rows.next(row);
      publisherId = publisher.get((readerInspector.getStructFieldData(row, fields.get(0))));
      advertiserId = advertiser.get(readerInspector.getStructFieldData(row, fields.get(1)));
      adInt = adUnit.get(readerInspector.getStructFieldData(row, fields.get(2)));
      time = timestamp.get(readerInspector.getStructFieldData(row, fields.get(3)));
      costField = cost.get(readerInspector.getStructFieldData(row, fields.get(4)));
      revenueField = revenue.get(readerInspector.getStructFieldData(row, fields.get(5)));
      impressionsField = impressions.get(readerInspector.getStructFieldData(row, fields.get(6)));
      clicksField = clicks.get(readerInspector.getStructFieldData(row, fields.get(6)));
      System.out.println("row 2 is" + "publisher Id is " + publisherId + "advertiserId is " + advertiserId + "adInt is " + adInt + "time is" + time + "cost is" + costField + "revenue is" + revenueField + "impressions is" + impressionsField + "clicks is" + clicksField);

    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(2, stats[1].getNumberOfValues());

  }


  public void test2() throws Exception
  {
    ObjectInspector inspector;
    BytesWritable bytes1;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(SimpleStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePathBytes,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .bufferSize(10000));
    writer.addRow(new SimpleStruct(bytes(0, 1, 2, 3, 4)));
    writer.addRow(new SimpleStruct(bytes(0, 1, 2, 3)));
    writer.close();
    Reader reader = OrcFile.createReader(fs, testFilePathBytes);
    StructObjectInspector readerInspector = (StructObjectInspector)reader.getObjectInspector();
    BinaryObjectInspector bi = (BinaryObjectInspector)readerInspector.getStructFieldRef("bytes1").getFieldObjectInspector();
    // ByteObjectInspector by = (ByteObjectInspector) readerInspector.getStructFieldRef("byte1").getFieldObjectInspector();
    List<? extends StructField> fields = readerInspector.getAllStructFieldRefs();
    RecordReader rows = reader.rows(null);
    Object row = rows.next(null);
    System.out.println(bi.getPrimitiveWritableObject(readerInspector.getStructFieldData(row, fields.get(0))));
    // System.out.println(bi.getPrimitiveWritableObject(readerInspector.getStructFieldData(row,fields.get(1))));
    assertEquals(bytes(0, 1, 2, 3, 4), bi.getPrimitiveWritableObject(readerInspector.getStructFieldData(row, fields.get(0))));
    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    System.out.println("number of values: " + stats[0].getNumberOfValues());
    assertEquals(2, stats[0].getNumberOfValues());
    assertEquals("count: 2", stats[0].toString());
    row = rows.next(row);
    System.out.println(bi.getPrimitiveWritableObject(readerInspector.getStructFieldData(row, fields.get(0))));
    assertEquals(bytes(0, 1, 2, 3), bi.getPrimitiveWritableObject(
                 readerInspector.getStructFieldData(row, fields.get(0))));
  }

}
