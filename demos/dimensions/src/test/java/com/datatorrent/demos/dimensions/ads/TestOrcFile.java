package com.datatorrent.demos.dimensions.ads;


import java.io.File;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.ColumnStatistics;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.junit.*;
import static org.junit.Assert.assertEquals;
import org.junit.rules.TestName;


public class TestOrcFile
{
Path workDir = new Path(System.getProperty("test.tmp.dir","target" + File.separator + "test" + File.separator + "tmp"));

	  Configuration conf;
	  FileSystem fs;
	  Path testFilePath;

	  @Rule
	  public TestName testCaseName = new TestName();

	  @Before
	  public void openFileSystem () throws Exception {
	    conf = new Configuration();
	    fs = FileSystem.getLocal(conf);
	    testFilePath = new Path(workDir, "TestOrcFile." +
	        testCaseName.getMethodName() + ".orc");
	    fs.delete(testFilePath, false);
	  }

   @Test
   public void test1() throws Exception {
	    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
	      inspector = ObjectInspectorFactory.getReflectionObjectInspector
	          (AdInfo.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
	    }
	    Writer writer = OrcFile.createWriter(testFilePath,
	                                         OrcFile.writerOptions(conf)
	                                         .inspector(inspector)
	                                         .stripeSize(100000)
	                                         .bufferSize(10000));
      AdInfo adinfo = new AdInfo();
      adinfo.setAdUnit(12);
      adinfo.setAdvertiserId(40);
      adinfo.setClicks(20000L);
      adinfo.setCost(24.50);
      adinfo.setImpressions(4000L);
      adinfo.setRevenue(45.60);
      adinfo.setPublisherId(34);
      adinfo.setTimestamp(450000L);
      writer.addRow(adinfo);
      writer.close();
      Reader reader = OrcFile.createReader(fs, testFilePath);
	    // check the stats
       ColumnStatistics[] stats = reader.getStatistics();
       assertEquals(1, stats[1].getNumberOfValues());

}
}
