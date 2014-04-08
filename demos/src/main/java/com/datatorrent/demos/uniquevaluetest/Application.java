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

