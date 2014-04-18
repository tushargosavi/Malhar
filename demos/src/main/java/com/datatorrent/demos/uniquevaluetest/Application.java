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
package com.datatorrent.demos.uniquevaluetest;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.algo.UniqueValueCount;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

/**
 * Application to demonstrate UniqueValueCount partitionable operator. <br>
 * The input operator generate random key-value pairs, which is sent to
 * UniqueValueCount operator initially partitaioned into three partitions to
 * test unifier functionality, and output of the operator is sent to consoleOuput.
 */
public class Application implements StreamingApplication {

    @Override
    public void populateDAG(DAG dag, Configuration entries) {

        dag.setAttribute(dag.APPLICATION_NAME, "UniqueValueCountDemo");
        dag.setAttribute(dag.DEBUG, true);

        /* Generate random key-value pairs */
        RandomKeyValues randGen = dag.addOperator("randgen", new RandomKeyValues());

        /* Initialize with four partition to start with */
        UniqueValueCount<String> uniqCount = dag.addOperator("uniqevalue", new UniqueValueCount<String>());
        dag.setAttribute(uniqCount, Context.OperatorContext.INITIAL_PARTITION_COUNT, 3);

        /* output values to console */
        ConsoleOutputOperator output = dag.addOperator("output", new ConsoleOutputOperator());

        dag.addStream("stream1", randGen.outport, uniqCount.inputPort);
        dag.addStream("stream2", uniqCount.outputPort, output.input);
    }
}

