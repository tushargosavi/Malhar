package com.datatorrent.demos.pi;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.lib.stream.DevNullCounter;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="KafkaApp")
public class KafkaApp implements StreamingApplication
{
  @Override public void populateDAG(DAG dag, Configuration configuration)
  {
    KafkaSinglePortStringInputOperator kafka = dag.addOperator("Kafka", new KafkaSinglePortStringInputOperator());
    kafka.setZookeeper("localhost:2181");
    kafka.setTopic("test");
    DevNullCounter<String> counter = dag.addOperator("Counter", new DevNullCounter<String>());
    dag.addStream("s1", kafka.outputPort, counter.data);
  }
}
