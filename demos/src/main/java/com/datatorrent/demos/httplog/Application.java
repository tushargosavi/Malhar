package com.datatorrent.demos.httplog;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.demos.rollingtopwords.WindowedTopCounter;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;


public class Application implements StreamingApplication {
    private Locality locality = null;

    public void populateDAG(DAG dag, Configuration conf) {
        locality = Locality.CONTAINER_LOCAL;

        HttpLogReader reader = dag.addOperator("logreader", new HttpLogReader());

        LogUrlExtractor urlExtractor = dag.addOperator("urlextractor", new LogUrlExtractor());

        UniqueCounter<String> counter = dag.addOperator("counter", new UniqueCounter<String>());

        WindowedTopCounter<String> topCounts = dag.addOperator("TopCounter", new WindowedTopCounter<String>());
        topCounts.setTopCount(10);
        topCounts.setSlidingWindowWidth(600, 1);

        ConsoleOutputOperator co = dag.addOperator("output", new ConsoleOutputOperator());
        co.setStringFormat("console op" + ": %s");

        dag.addStream("stream1", reader.outputPort, urlExtractor.input);
        dag.addStream("stream2", urlExtractor.url, counter.data);
        dag.addStream("stream3", counter.count, topCounts.input);
        dag.addStream("stream4", topCounts.output, co.input);

    }
}
