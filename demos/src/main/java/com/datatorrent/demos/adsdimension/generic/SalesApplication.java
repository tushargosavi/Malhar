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
package com.datatorrent.demos.adsdimension.generic;

import java.util.List;
import java.util.Map;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.hds.tfile.TFileImpl;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.contrib.kafka.SimpleKafkaConsumer;

/**
 * DimensionsDemo run with HDS
 *
 * Example of configuration
 <pre>
 {@code
 <property>
 <name>dt.application.SalesApplication.class</name>
 <value>com.datatorrent.demos.adsdimension.GenericApplication</value>
 </property>

 <property>
 <name>dt.application.SalesApplication.attr.containerMemoryMB</name>
 <value>8192</value>
 </property>

 <property>
 <name>dt.application.SalesApplication.attr.containerJvmOpts</name>
 <value>-Xmx6g -server -Dlog4j.debug=true -Xloggc:&lt;LOG_DIR&gt;/gc.log -verbose:gc -XX:+PrintGCDateStamps</value>
 </property>

 <property>
 <name>dt.application.SalesApplication.port.*.attr.QUEUE_CAPACITY</name>
 <value>32000</value>
 </property>

 <property>
 <name>dt.operator.InputGenerator.attr.INITIAL_PARTITION_COUNT</name>
 <value>8</value>
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
 <name>dt.operator.Store.attr.INITIAL_PARTITION_COUNT</name>
 <value>4</value>
 </property>

 <property>
 <name>dt.operator.Store.fileStore.basePath</name>
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
@ApplicationAnnotation(name="SalesApplication")
public class SalesApplication implements StreamingApplication
{

  public static EventSchema getDataDesc() {
    EventSchema eDesc = new EventSchema();

    Map<String, Class<?>> dataDesc  = Maps.newHashMap();
    dataDesc.put("timestamp", Long.class);
    dataDesc.put("productId", Integer.class);
    dataDesc.put("customerId", Integer.class);
    dataDesc.put("channelId", Integer.class);

    dataDesc.put("amount", Long.class);
    eDesc.setDataDesc(dataDesc);

    String[] keys = { "timestamp", "productId", "customerId", "channelId" };
    List<String> keyDesc = Lists.newArrayList(keys);
    eDesc.setKeys(keyDesc);

    Map<String, String> aggrDesc = Maps.newHashMap();
    aggrDesc.put("amount", "sum");
    eDesc.setAggrDesc(aggrDesc);

    return eDesc;
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    EventSchema dataDesc = getDataDesc();
    dag.setAttribute(DAG.APPLICATION_NAME, "SalesApplication");

    JsonSalesInfoGenerator input = dag.addOperator("InputGenerator", JsonSalesInfoGenerator.class);
    JsonToMapConverter converter = dag.addOperator("Converter", JsonToMapConverter.class);

    GenericDimensionComputation dimensions = dag.addOperator("DimensionsComputation", new GenericDimensionComputation());
    dimensions.setSchema(dataDesc);

    DimensionStoreOperator store = dag.addOperator("Store", DimensionStoreOperator.class);
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    store.setFileStore(hdsFile);
    store.setEventDesc(dataDesc);
    store.setAggregator(new MapAggregator(dataDesc));

    KafkaSinglePortStringInputOperator queries = dag.addOperator("Query", new KafkaSinglePortStringInputOperator());
    queries.setConsumer(new SimpleKafkaConsumer());

    KafkaSinglePortOutputOperator<Object, Object> queryResult = dag.addOperator("QueryResult", new KafkaSinglePortOutputOperator<Object, Object>());
    queryResult.getConfigProperties().put("serializer.class", com.datatorrent.demos.adsdimension.KafkaJsonEncoder.class.getName());

    dag.addStream("JSONStream", input.jsonOutput, converter.input).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("MapStream", converter.outputMap, dimensions.data).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, store.input);
    dag.addStream("Query", queries.outputPort, store.query);
    dag.addStream("QueryResult", store.queryResult, queryResult.inputPort);
  }

}

