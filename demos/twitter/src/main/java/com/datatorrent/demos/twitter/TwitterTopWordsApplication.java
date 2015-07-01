/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.twitter;


import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.twitter.TwitterSampleInput;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.snapshot.AppDataSnapshotServerMap;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.google.common.collect.Maps;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;



/**
 * This application is same as other twitter demo
 * {@link com.datatorrent.demos.twitter.TwitterTopCounterApplication} <br>
 * Run Sample :
 *
 * <pre>
 * 2013-06-17 16:50:34,911 [Twitter Stream consumer-1[Establishing connection]] INFO  twitter4j.TwitterStreamImpl info - Connection established.
 * 2013-06-17 16:50:34,912 [Twitter Stream consumer-1[Establishing connection]] INFO  twitter4j.TwitterStreamImpl info - Receiving status stream.
 * topWords: {}
 * topWords: {love=1, ate=1, catch=1, calma=1, Phillies=1, ela=1, from=1, running=1}
 * </pre>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name="RollingTopWordsDemo")
public class TwitterTopWordsApplication implements StreamingApplication
{
  private static final Logger logger = LoggerFactory.getLogger(TwitterTopWordsApplication.class);

  public static final String TABULAR_SCHEMA = "twitterWordDataSchema.json";
  public static final String CONVERSION_SCHEMA = "twitterWordConverterSchema.json";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    Operator.OutputPort<String> queryPort = null;
    Operator.InputPort<String> queryResultPort = null;

    String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
    URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");
    logger.info("WebSocket with gateway at: {}", gatewayAddress);
    PubSubWebSocketAppDataQuery wsIn = dag.addOperator("Query", new PubSubWebSocketAppDataQuery());
    wsIn.setUri(uri);
    queryPort = wsIn.outputPort;
    PubSubWebSocketAppDataResult wsOut = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());
    wsOut.setUri(uri);
    queryResultPort = wsOut.input;

    TwitterSampleInput twitterFeed = new TwitterSampleInput();
    twitterFeed = dag.addOperator("TweetSampler", twitterFeed);

    TwitterStatusWordExtractor wordExtractor = dag.addOperator("WordExtractor", TwitterStatusWordExtractor.class);
    UniqueCounter<String> uniqueCounter = dag.addOperator("UniqueWordCounter", new UniqueCounter<String>());
    WindowedTopCounter<String> topCounts = dag.addOperator("TopCounter", new WindowedTopCounter<String>());
    AppDataSnapshotServerMap tabularServer = dag.addOperator("Tabular Server", new AppDataSnapshotServerMap());

    Map<String, String> conversionMap = Maps.newHashMap();
    conversionMap.put("word", WindowedTopCounter.FIELD_TYPE);
    String tabularSchema = SchemaUtils.jarResourceFileToString(TABULAR_SCHEMA);

    tabularServer.setTabularSchemaJSON(tabularSchema);
    tabularServer.setTableFieldToMapField(conversionMap);

    logger.info("Tabular schema {}", tabularSchema);
    topCounts.setSlidingWindowWidth(120);
    topCounts.setDagWindowWidth(1);

    dag.addStream("TweetStream", twitterFeed.text, wordExtractor.input);
    dag.addStream("TwittedWords", wordExtractor.output, uniqueCounter.data);
    dag.addStream("UniqueWordCounts", uniqueCounter.count, topCounts.input).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("MapProvider", topCounts.output, tabularServer.input);
    dag.addStream("TopURLQuery", queryPort, tabularServer.query).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("TopURLResult", tabularServer.queryResult, queryResultPort);
  }
}
