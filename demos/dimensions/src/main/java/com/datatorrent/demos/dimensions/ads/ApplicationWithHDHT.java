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
package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.contrib.kafka.KafkaJsonEncoder;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.contrib.kafka.SimpleKafkaConsumer;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregator;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.PubSubWebSocketInputOperator;
import com.datatorrent.lib.io.PubSubWebSocketOutputOperator;
import com.datatorrent.lib.statistics.DimensionsComputation;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;

/**
 * An AdsDimensionsDemo run with HDHT
 *
 * Example of configuration
 <pre>
 {@code
 <property>
 <name>dt.application.AdsDimensionsWithHDSDemo.class</name>
 <value>com.datatorrent.demos.adsdimension.ApplicationWithHDS</value>
 </property>

 <property>
 <name>dt.application.AdsDimensionsWithHDSDemo.attr.containerMemoryMB</name>
 <value>8192</value>
 </property>

 <property>
 <name>dt.application.AdsDimensionsWithHDSDemo.attr.containerJvmOpts</name>
 <value>-Xmx6g -server -Dlog4j.debug=true -Xloggc:&lt;LOG_DIR&gt;/gc.log -verbose:gc -XX:+PrintGCDateStamps</value>
 </property>

 <property>
 <name>dt.application.AdsDimensionsWithHDSDemo.port.*.attr.QUEUE_CAPACITY</name>
 <value>32000</value>
 </property>

 <property>
 <name>dt.operator.InputGenerator.attr.PARTITIONER</name>
 <value>com.datatorrent.lib.partitioner.StatelessPartitioner:8</value>
 </property>

 <property>
 <name>dt.operator.DimensionsComputation.attr.APPLICATION_WINDOW_COUNT</name>
 <value>4</value>
 </property>

 <property>
 <name>dt.operator.DimensionsComputation.port.data.attr.PARTITION_PARALLEL</name>
 <value>true</value>
 </property>

 <property>
 <name>dt.operator.HDSOut.attr.PARTITIONER</name>
 <value>com.datatorrent.lib.partitioner.StatelessPartitioner:4</value>
 </property>

 <property>
 <name>dt.operator.HDSOut.fileStore.basePath</name>
 <value>AdsDimensionWithHDS</value>
 </property>

 <property>
 <name>dt.operator.Query.topic</name>
 <value>HDSQuery</value>
 </property>

 <property>
 <name>dt.operator.QueryResult.topic</name>
 <value>HDSQueryResult</value>
 </property>

 <property>
 <name>dt.operator.Query.brokerSet</name>
 <value>localhost:9092</value>
 </property>

 <property>
 <name>dt.operator.QueryResult.prop.configProperties(metadata.broker.list)</name>
 <value>localhost:9092</value>
 </property>

 }
 </pre>
 *
 */
@ApplicationAnnotation(name=ApplicationWithHDHT.APP_NAME)
public class ApplicationWithHDHT implements StreamingApplication
{
  public static final String APP_NAME = "AdsDimensionsDemoWithHDHT";
  public static final String PROP_USE_WEBSOCKETS = "dt.application." + APP_NAME + ".useWebSockets";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    InputItemGenerator input = dag.addOperator("InputGenerator", InputItemGenerator.class);
    DimensionsComputation<AdInfo, AdInfo.AdInfoAggregateEvent> dimensions = dag.addOperator("DimensionsComputation", new DimensionsComputation<AdInfo, AdInfo.AdInfoAggregateEvent>());
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
    String[] dimensionSpecs = new String[] {
        "time=" + TimeUnit.MINUTES,
        "time=" + TimeUnit.MINUTES + ":adUnit",
        "time=" + TimeUnit.MINUTES + ":advertiserId",
        "time=" + TimeUnit.MINUTES + ":publisherId",
        "time=" + TimeUnit.MINUTES + ":advertiserId:adUnit",
        "time=" + TimeUnit.MINUTES + ":publisherId:adUnit",
        "time=" + TimeUnit.MINUTES + ":publisherId:advertiserId",
        "time=" + TimeUnit.MINUTES + ":publisherId:advertiserId:adUnit"
    };

    AdInfoAggregator[] aggregators = new AdInfoAggregator[dimensionSpecs.length];
    for (int i = dimensionSpecs.length; i-- > 0;) {
      AdInfoAggregator aggregator = new AdInfoAggregator();
      aggregator.init(dimensionSpecs[i]);
      aggregators[i] = aggregator;
    }
    dimensions.setAggregators(aggregators);

    AdsDimensionStoreOperator store = dag.addOperator("Store", AdsDimensionStoreOperator.class);
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    store.setFileStore(hdsFile);
    store.setAggregator(new AdInfoAggregator());
    dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator< MutableLong >());

    Operator.OutputPort<String> queryPort;
    Operator.InputPort<Object> queryResultPort;
    if (conf.getBoolean(PROP_USE_WEBSOCKETS,  false)) {
      String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
      URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");
      //LOG.info("WebSocket with gateway at: {}", gatewayAddress);
      PubSubWebSocketInputOperator<String> wsIn = dag.addOperator("Query", new PubSubWebSocketInputOperator<String>());
      wsIn.setUri(uri);
      queryPort = wsIn.outputPort;
      PubSubWebSocketOutputOperator<Object> wsOut = dag.addOperator("QueryResult", new PubSubWebSocketOutputOperator<Object>());
      wsOut.setUri(uri);
      queryResultPort = wsOut.input;
    } else {
      KafkaSinglePortStringInputOperator queries = dag.addOperator("Query", new KafkaSinglePortStringInputOperator());
      queries.setConsumer(new SimpleKafkaConsumer());
      queryPort = queries.outputPort;
      KafkaSinglePortOutputOperator<Object, Object> queryResult = dag.addOperator("QueryResult", new KafkaSinglePortOutputOperator<Object, Object>());
      queryResult.getConfigProperties().put("serializer.class", KafkaJsonEncoder.class.getName());
      queryResultPort = queryResult.inputPort;
    }

    dag.addStream("InputStream", input.outputPort, dimensions.data).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, store.input);
    dag.addStream("Query", queryPort, store.query);
    dag.addStream("QueryResult", store.queryResult, queryResultPort);
  }

}

