/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.uniquecount;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.algo.PartitionableUniqueCount;
import com.datatorrent.lib.partitioner.StatelessPartitioner;
import com.datatorrent.lib.algo.UniqueCounterValue;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.stream.StreamDuplicater;
import com.datatorrent.lib.util.KeyHashValPair;
import org.apache.hadoop.conf.Configuration;

/**
 * Application to demonstrate PartitionableUniqueCount operator. <br>
 * The input operator generate random keys, which is sent to
 * PartitionableUniqueCount operator initially partitioned into three partitions to
 * test unifier functionality, and output of the operator is sent to verifier to verify
 * that it generates correct result.
 *
 * @since 1.0.2
 */
@ApplicationAnnotation(name="UniqueValueCountDemo")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration entries)
  {
        /* Generate random key-value pairs */
    RandomKeysGenerator randGen = dag.addOperator("randomgen", new RandomKeysGenerator());


        /* Initialize with three partition to start with */
    // UniqueCount1 uniqCount = dag.addOperator("uniqevalue", new UniqueCount1());
    PartitionableUniqueCount<Integer> uniqCount = dag.addOperator("uniqevalue", new PartitionableUniqueCount<Integer>());

    uniqCount.setCumulative(false);
    dag.setAttribute(uniqCount, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<PartitionableUniqueCount<Integer>>(3));

    CountVerifier<Integer> verifier = dag.addOperator("verifier", new CountVerifier<Integer>());
    StreamDuplicater<KeyHashValPair<Integer, Integer>> dup = dag.addOperator("dup", new StreamDuplicater<KeyHashValPair<Integer, Integer>>());
    ConsoleOutputOperator output = dag.addOperator("output", new ConsoleOutputOperator());

    ConsoleOutputOperator successOutput = dag.addOperator("successoutput", new ConsoleOutputOperator());
    successOutput.setStringFormat("Success %d");
    ConsoleOutputOperator failureOutput = dag.addOperator("failureoutput", new ConsoleOutputOperator());
    failureOutput.setStringFormat("Failure %d");

    // success and failure counters.
    UniqueCounterValue<Integer> successcounter = dag.addOperator("successcounter", new UniqueCounterValue<Integer>());
    UniqueCounterValue<Integer> failurecounter = dag.addOperator("failurecounter", new UniqueCounterValue<Integer>());

    dag.addStream("datain", randGen.outPort, uniqCount.data);
    dag.addStream("dataverification0", randGen.verificationPort, verifier.in1);
    dag.addStream("split", uniqCount.count, dup.data);
    dag.addStream("consoutput", dup.out1, output.input);
    dag.addStream("dataverification1", dup.out2, verifier.in2);
    dag.addStream("successc", verifier.successPort, successcounter.data);
    dag.addStream("failurec", verifier.failurePort, failurecounter.data);
    dag.addStream("succconsoutput", successcounter.count, successOutput.input);
    dag.addStream("failconsoutput", failurecounter.count, failureOutput.input);
  }
}
