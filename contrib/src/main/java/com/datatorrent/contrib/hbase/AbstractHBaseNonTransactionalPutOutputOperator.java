/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.hbase;

import com.datatorrent.api.annotation.ShipContainingJars;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.db.AbstractStoreOutputOperator;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InterruptedIOException;

/**
 * Operator for storing tuples in HBase rows.<br>
 * 
 * <br>
 * This class provides a HBase output operator that can be used to store tuples
 * in rows in a HBase table. It should be extended by the end-operator
 * developer. The extending class should implement operationPut method and
 * provide a HBase Put metric object that specifies where and what to store for
 * the tuple in the table.<br>
 * 
 * <br>
 * This class offers non-transactional put where tuples are put as they come in
 * 
 * @param <T>
 *            The tuple type
 * @since 1.0.2
 */
@ShipContainingJars(classes = { org.apache.hadoop.hbase.client.HTable.class,
		org.apache.hadoop.hbase.util.BloomFilterFactory.class,
		com.google.protobuf.AbstractMessageLite.class,
		org.apache.hadoop.hbase.BaseConfigurable.class,
		org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.class,
		org.apache.hadoop.hbase.ipc.BadAuthException.class,
		org.cloudera.htrace.HTraceConfiguration.class })
public abstract class AbstractHBaseNonTransactionalPutOutputOperator<T> extends
		AbstractStoreOutputOperator<T, HBaseStore> {
	private static final transient Logger logger = LoggerFactory
			.getLogger(AbstractHBaseNonTransactionalPutOutputOperator.class);

	public AbstractHBaseNonTransactionalPutOutputOperator() {
		store = new HBaseStore();
	}

	@Override
	public void processTuple(T tuple) {
		HTable table = store.getTable();
		Put put = operationPut(tuple);
		try {
			table.put(put);
		} catch (RetriesExhaustedWithDetailsException e) {
			logger.error("Could not output tuple", e);
			DTThrowable.rethrow(e);
		} catch (InterruptedIOException e) {
			logger.error("Could not output tuple", e);
			DTThrowable.rethrow(e);
		}

	}

	public abstract Put operationPut(T t);

}
