package com.datatorrent.demos.pi;

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import java.util.Random;

class Generator extends BaseOperator implements InputOperator
{
  public transient DefaultOutputPort<String> out = new DefaultOutputPort<String>();
  private Random r = new Random();
  private byte[] bytes;

  @Override public void emitTuples()
  {
    r.nextBytes(bytes);
    out.emit(new String(bytes));
  }

  @Override public void setup(Context.OperatorContext context)
  {
    r = new Random();
    bytes = new byte[100];
  }
}

class FsWriter extends AbstractFileOutputOperator<String>
{
  Random r = new Random();

  @Override protected String getFileName(String tuple)
  {
    int id = r.nextInt(5);
    return "file-" + id;
  }

  @Override protected byte[] getBytesForTuple(String tuple)
  {
    return (tuple + "\n").getBytes();
  }
}

@ApplicationAnnotation(name="WriteFailureTest")
public class WriteFailureTest implements StreamingApplication
{
  @Override public void populateDAG(DAG dag, Configuration conf)
  {
    Generator gen = dag.addOperator("Gen", new Generator());
    FsWriter store = dag.addOperator("Store", new FsWriter());
    store.setFilePath("StoreTest");
    dag.addStream("s1",gen.out, store.input);
  }
}
