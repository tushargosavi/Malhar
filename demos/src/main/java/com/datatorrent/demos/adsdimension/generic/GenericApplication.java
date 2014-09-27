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
 <name>dt.application.AdsDimensionsGenericDemo.class</name>
 <value>com.datatorrent.demos.adsdimension.GenericApplication</value>
 </property>

 <property>
 <name>dt.application.AdsDimensionsGenericDemo.attr.containerMemoryMB</name>
 <value>8192</value>
 </property>

 <property>
 <name>dt.application.AdsDimensionsGenericDemo.attr.containerJvmOpts</name>
 <value>-Xmx6g -server -Dlog4j.debug=true -Xloggc:&lt;LOG_DIR&gt;/gc.log -verbose:gc -XX:+PrintGCDateStamps</value>
 </property>

 <property>
 <name>dt.application.AdsDimensionsGenericDemo.port.*.attr.QUEUE_CAPACITY</name>
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
@ApplicationAnnotation(name="AdsDimensionsGenericDemo")
public class GenericApplication implements StreamingApplication
{

  public static EventSchema getEventSchema() {
    EventSchema eventSchema = new EventSchema();

    Map<String, Class<?>> fieldTypes  = Maps.newHashMap();
    fieldTypes.put("timestamp", Long.class);
    fieldTypes.put("publisherId", Integer.class);
    fieldTypes.put("advertiserId", Integer.class);
    fieldTypes.put("adUnit", Integer.class);

    fieldTypes.put("clicks", Long.class);
    eventSchema.setFieldTypes(fieldTypes);

    String[] keys = { "timestamp", "publisherId", "advertiserId", "adUnit" };
    List<String> keyDesc = Lists.newArrayList(keys);
    eventSchema.setKeys(keyDesc);

    Map<String, String> aggregates = Maps.newHashMap();
    aggregates.put("clicks", "sum");
    eventSchema.setAggregates(aggregates);

    return eventSchema;
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    EventSchema eventSchema = getEventSchema();
    dag.setAttribute(DAG.APPLICATION_NAME, "AdsDimensionsGeneric");

    JsonAdInfoGenerator input = dag.addOperator("InputGenerator", JsonAdInfoGenerator.class);
    JsonToMapConverter converter = dag.addOperator("Converter", JsonToMapConverter.class);

    GenericDimensionComputation dimensions = dag.addOperator("DimensionsComputation", new GenericDimensionComputation());
    dimensions.setSchema(eventSchema);

    DimensionStoreOperator store = dag.addOperator("Store", DimensionStoreOperator.class);
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    store.setFileStore(hdsFile);
    store.setEventDesc(eventSchema);
    store.setAggregator(new MapAggregator(eventSchema));

    KafkaSinglePortStringInputOperator queries = dag.addOperator("Query", new KafkaSinglePortStringInputOperator());
    queries.setConsumer(new SimpleKafkaConsumer());

    KafkaSinglePortOutputOperator<Object, Object> queryResult = dag.addOperator("QueryResult", new KafkaSinglePortOutputOperator<Object, Object>());
    queryResult.getConfigProperties().put("serializer.class", com.datatorrent.demos.adsdimension.KafkaJsonEncoder.class.getName());

    // Removing setLocality(Locality.CONTAINER_LOCAL) from JSONStream and MapStream to isolate performance bottleneck
    dag.addStream("JSONStream", input.jsonOutput, converter.input);
    dag.addStream("MapStream", converter.outputMap, dimensions.data);
    dag.addStream("DimensionalData", dimensions.output, store.input);
    dag.addStream("Query", queries.outputPort, store.query);
    dag.addStream("QueryResult", store.queryResult, queryResult.inputPort);
  }

}
