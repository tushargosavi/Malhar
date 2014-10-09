package com.datatorrent.demos.adsdimension.waltest;

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.hds.tfile.TFileImpl;
import org.apache.hadoop.conf.Configuration;
import com.datatorrent.api.Context.PortContext;

@ApplicationAnnotation(name="HDSWalBenchmark")
public class WalTestApplication implements StreamingApplication
{
  @Override public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.APPLICATION_NAME, "HDSWalBenchmark");

    Generator gen = dag.addOperator("Generator", new Generator());
    HDSOperator hdsOut = dag.addOperator("HDSStore", new HDSOperator());

    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsOut.setFileStore(hdsFile);
    hdsOut.setMaxWalFileSize(2 * 1024 * 1024);

    dag.addStream("s1", gen.out, hdsOut.input);
  }
}
