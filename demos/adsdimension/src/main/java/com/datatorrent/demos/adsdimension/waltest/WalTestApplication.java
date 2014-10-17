package com.datatorrent.demos.adsdimension.waltest;

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.hds.HDSStatCounters;
import com.datatorrent.contrib.hds.tfile.TFileImpl;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.fs.AbstractFSDirectoryInputOperator;
import org.apache.commons.lang.mutable.MutableLong;
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
    hdsFile.setBasePath("WALBenchMarkDir");
    hdsOut.setFileStore(hdsFile);
    hdsOut.setMaxWalFileSize(2 * 1024 * 1024);
    dag.getOperatorMeta("HDSStore").getAttributes().put(Context.OperatorContext.COUNTERS_AGGREGATOR,
        new BasicCounters.LongAggregator<MutableLong>());

    dag.addStream("s1", gen.out, hdsOut.input);
  }
}
