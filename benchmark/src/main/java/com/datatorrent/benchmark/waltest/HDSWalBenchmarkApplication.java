/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.benchmark.waltest;

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.hds.tfile.TFileImpl;
import com.datatorrent.lib.counters.BasicCounters;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.lang.mutable.MutableLong;


@ApplicationAnnotation(name="HDSWalBenchmarkApplication")
public class HDSWalBenchmarkApplication implements StreamingApplication
{
  @Override public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.APPLICATION_NAME, "HDSWalBenchmarkApplication");

    Generator gen = dag.addOperator("Generator", new Generator());
    gen.setTupleBlast(1000);
    gen.setSleepms(100);
    dag.getOperatorMeta("Generator").getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 1);

    HDSOperator hdsOut = dag.addOperator("HDSStore1", new HDSOperator());

    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsFile.setBasePath("WALBenchMarkDir");
    hdsOut.setFileStore(hdsFile);
    hdsOut.setMaxWalFileSize(2 * 1024 * 1024);
    dag.getOperatorMeta("HDSStore1").getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 1);
    dag.getOperatorMeta("HDSStore1").getAttributes().put(Context.OperatorContext.COUNTERS_AGGREGATOR,
        new BasicCounters.LongAggregator<MutableLong>());

    dag.addStream("s1", gen.out, hdsOut.input);
  }
}
