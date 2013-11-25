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
var BigInteger = require('jsbn');
var Collection = require('./LogicalOperatorCollection');
var WindowId = require('./WindowId');
describe('LogicalOperatorCollection.js', function() {
    
	var sandbox;

	beforeEach(function() {
	    sandbox = sinon.sandbox.create();
	});

	afterEach(function() {
	    sandbox.restore();
	});

	it('should create aggregate logical operator objects', function() {
		var lastHeartbeat = +new Date() - 500;
	    var operators = [
	    	// operator1
			{
			    "className": "com/datatorrent/lib/operator1", 
			    "container": "1", 
			    "cpuPercentageMA": "10", 
			    "currentWindowId": "98",
			    "recoveryWindowId": "74",
			    "failureCount": "1",
			    "host": "node0.morado.com", 
			    "id": "1", 
			    "ports": [{
			    	"bufferServerBytesPSMA": "1000",
					"name": "integer_data",
					"recordingStartTime": "-1",
					"totalTuples": "100",
					"tuplesPSMA": "10",
					"type": "output"
			    }],
			    "lastHeartbeat": lastHeartbeat, 
			    "latencyMA": "24", 
			    "logicalName": "op1",
			    "recordingStartTime": "-1",
			    "status": "ACTIVE", 
			    "totalTuplesEmitted": "1000", 
			    "totalTuplesProcessed": "1000", 
			    "tuplesEmittedPSMA": "10", 
			    "tuplesProcessedPSMA": "10"
			},
		    {
		        "className": "com/datatorrent/lib/operator1", 
		        "container": "2", 
		        "cpuPercentageMA": "9", 
		        "currentWindowId": "99",
		        "recoveryWindowId": "75",
		        "failureCount": "0", 
		        "host": "node1.morado.com", 
		        "id": "2", 
		        "ports": [{
		        	"bufferServerBytesPSMA": "900",
					"name": "integer_data",
					"recordingStartTime": "-1",
					"totalTuples": "110",
					"tuplesPSMA": "11",
					"type": "output"
		        }], 
		        "lastHeartbeat": lastHeartbeat -1, 
		        "latencyMA": "25", 
		        "logicalName": "op1",
		        "recordingStartTime": "-1",
		        "status": "ACTIVE", 
		        "totalTuplesEmitted": "1100", 
		        "totalTuplesProcessed": "1100", 
		        "tuplesEmittedPSMA": "11", 
		        "tuplesProcessedPSMA": "11"
		    },
		    {
		        "className": "com/datatorrent/lib/operator1", 
		        "container": "3", 
		        "cpuPercentageMA": "11", 
		        "currentWindowId": "100",
		        "recoveryWindowId": "75",
		        "failureCount": "3", 
		        "host": "node1.morado.com", 
		        "id": "3", 
		        "ports": [{
		        	"bufferServerBytesPSMA": "1100",
					"name": "integer_data",
					"recordingStartTime": "-1",
					"totalTuples": "90",
					"tuplesPSMA": "9",
					"type": "output"
		        }],
		        "lastHeartbeat": lastHeartbeat -2, 
		        "latencyMA": "26", 
		        "logicalName": "op1",
		        "recordingStartTime": "-1",
		        "status": "INACTIVE", 
		        "totalTuplesEmitted": "900", 
		        "totalTuplesProcessed": "900", 
		        "tuplesEmittedPSMA": "9", 
		        "tuplesProcessedPSMA": "9"
		    },
		    // operator2
		    {
		        "className": "com/datatorrent/lib/operator2", 
		        "container": "3", 
		        "cpuPercentageMA": "15", 
		        "currentWindowId": "99",
		        "recoveryWindowId": "75",
		        "failureCount": "1", 
		        "host": "node2.morado.com", 
		        "id": "4", 
		        "ports": [
		        	{
		        		"bufferServerBytesPSMA": "100",
						"name": "port1",
						"recordingStartTime": "-1",
						"totalTuples": "200",
						"tuplesPSMA": "20",
						"type": "input"
		        	},
		        	{
		        		"bufferServerBytesPSMA": "50",
						"name": "port2",
						"recordingStartTime": "-1",
						"totalTuples": "300",
						"tuplesPSMA": "30",
						"type": "output"
		        	}
		        ],
		        "lastHeartbeat": lastHeartbeat, 
		        "latencyMA": "24", 
		        "logicalName": "op2",
		        "recordingStartTime": "-1",
		        "status": "ACTIVE", 
		        "totalTuplesEmitted": "1200", 
		        "totalTuplesProcessed": "1200", 
		        "tuplesEmittedPSMA": "12", 
		        "tuplesProcessedPSMA": "12"
		    },
		    {
		        "className": "com/datatorrent/lib/operator2", 
		        "container": "4", 
		        "cpuPercentageMA": "13", 
		        "currentWindowId": "100",
		        "recoveryWindowId": "76",
		        "failureCount": "0", 
		        "host": "node2.morado.com", 
		        "id": "5", 
		        "ports": [
		        	{
		        		"bufferServerBytesPSMA": "100",
						"name": "port1",
						"recordingStartTime": "-1",
						"totalTuples": "200",
						"tuplesPSMA": "20",
						"type": "input"
		        	},
		        	{
		        		"bufferServerBytesPSMA": "50",
						"name": "port2",
						"recordingStartTime": "-1",
						"totalTuples": "300",
						"tuplesPSMA": "30",
						"type": "output"
		        	}
		        ], 
		        "lastHeartbeat": lastHeartbeat - 3, 
		        "latencyMA": "24", 
		        "logicalName": "op2",
		        "recordingStartTime": "-1",
		        "status": "INACTIVE", 
		        "totalTuplesEmitted": "1000",
		        "totalTuplesProcessed": "1000",
		        "tuplesEmittedPSMA": "10", 
		        "tuplesProcessedPSMA": "10"
		    }
	    ]
		
		var response = Collection.prototype.responseTransform({ operators: operators });

		expect(response).to.eql([
			{
				// same
				"className": "com/datatorrent/lib/operator1", 
				// array of all
		        "containers": ["1","2","3"],
		        // sum
		        "cpuPercentageMA": 30,
		        // min
		        "currentWindowId": new WindowId("98"),
		        // min
		        "recoveryWindowId": new WindowId("74"),
		        // sum
		        "failureCount": 4,
		        // array of all
		        "hosts": ["node0.morado.com","node1.morado.com"], 
		        // array of all
		        "ids": ["1","2","3"],
		        "ports": [{
		        	// sum
		        	"bufferServerBytesPSMA": new BigInteger("3000"),
		        	// same
					"name": "integer_data",
					// sum
					"totalTuples": new BigInteger("300"),
					// sum
					"tuplesPSMA": new BigInteger("30"),
					// same
					"type": "output"
		        }],
		        // min
		        "lastHeartbeat": lastHeartbeat -2, 
		        // max
		        "latencyMA": 26, 
		        // same
		        "logicalName": "op1",
		        // map of status => array of ids
		        "status": {"ACTIVE": ["1", "2"], "INACTIVE": ["3"] }, 
		        // sum
		        "totalTuplesEmitted": new BigInteger("3000"), 
		        "totalTuplesProcessed": new BigInteger("3000"), 
		        "tuplesEmittedPSMA": new BigInteger("30"), 
		        "tuplesProcessedPSMA": new BigInteger("30")		
			},
			{
				// same
				"className": "com/datatorrent/lib/operator2", 
				// array of all
		        "containers": ["3","4"],
		        // sum
		        "cpuPercentageMA": 28,
		        // min
		        "currentWindowId": new WindowId("99"),
		        // min
		        "recoveryWindowId": new WindowId("75"),
		        // sum
		        "failureCount": 1,
		        // array of all
		        "hosts": ["node2.morado.com"], 
		        // array of all
		        "ids": ["4","5"],
		        "ports": [
		        	{
		        		// sum
		        		"bufferServerBytesPSMA": new BigInteger("200"),
		        		// same
						"name": "port1",
						// sum
						"totalTuples": new BigInteger("400"),
						// sum
						"tuplesPSMA": new BigInteger("40"),
						// same
						"type": "input"
		        	},
		        	{
		        		"bufferServerBytesPSMA": new BigInteger("100"),
						"name": "port2",
						"totalTuples": new BigInteger("600"),
						"tuplesPSMA": new BigInteger("60"),
						"type": "output"
		        	}
		        ],
		        // min
		        "lastHeartbeat": lastHeartbeat -3, 
		        // max
		        "latencyMA": 26, 
		        // same
		        "logicalName": "op2",
		        // map of status => array of ids
		        "status": {"ACTIVE": ["4"], "INACTIVE": ["5"] }, 
		        // sum
		        "totalTuplesEmitted": new BigInteger("2200"), 
		        "totalTuplesProcessed": new BigInteger("2200"), 
		        "tuplesEmittedPSMA": new BigInteger("22"), 
		        "tuplesProcessedPSMA": new BigInteger("22")
			}

		]);

	});

});