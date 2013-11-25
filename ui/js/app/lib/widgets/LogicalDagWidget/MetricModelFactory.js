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
var _ = require('underscore');
var Backbone = require('backbone');
var MetricModel = require('./MetricModel');
var BigInteger = require('jsbn');
var formatters = DT.formatters;
var bormat = require('bormat');

var MetricModelFactory = {
    getMetricModel: function (name, operators) {
        return new MetricModel(null, {
            operators: operators,
            implementation: this[name]
        });
    },

    latency: {
        update: function (id, operator, map) {
            if (map.hasOwnProperty(id)) {
                map[id] = Math.max(operator.get('latencyMA'), map[id]);
            } else {
                map[id] = operator.get('latencyMA');
            }
        },
        showMetric: function (id, map) {
            var value = map[id];
            return (map.hasOwnProperty(id) && value > 0);
        }
    },

    partitions: {
        update: function (id, operator, map) {
            var unifierClass = operator.get('unifierClass');

            if (!unifierClass) { // do not count unifiers
                if (map.hasOwnProperty(id)) {
                    map[id]++;
                } else {
                    map[id] = 1;
                }
            }
        },
        showMetric: function (id, map) {
            var value = map[id];
            return (map.hasOwnProperty(id) && value > 1);
        }
    },

    containers: {
        updateAll: function (operators, map) {
            var groups = operators.groupBy(function (operator) {
                return operator.get('logicalName');
            });

            for (var id in groups) {
                if (groups.hasOwnProperty(id)) {
                    var ops = groups[id];

                    var containers = _.groupBy(ops, function (operator) {
                        return operator.get('container');
                    });

                    map[id] = Object.keys(containers).length;
                }
            }
        },

        showMetric: function (id, map) {
            return map.hasOwnProperty(id);
        }
    },

    avgCpu: {
        updateAll: function (operators, map) {
            var sum = 0;
            var countMap = {};
            operators.each(function (operator) {
                var id = operator.get('logicalName');
                var cpu = parseFloat(operator.get('cpuPercentageMA'));
                if (map.hasOwnProperty(id)) {
                    map[id] += cpu;
                    countMap[id]++;
                } else {
                    map[id] = cpu;
                    countMap[id] = 1;
                }
            }.bind(this));

            for (var id in map) {
                if (map.hasOwnProperty(id)) {
                    map[id] = map[id] / countMap[id];
                }
            }
        },

        showMetric: function (id, map) {
            return map.hasOwnProperty(id);
        },

        valueToString: function (value) {
            return formatters.percentageFormatter(value, true);
        }
    },

    processed: {
        update: function (id, operator, map) {
            var total = new BigInteger(operator.get('tuplesProcessedPSMA'));

            if (map.hasOwnProperty(id)) {
                map[id] = map[id].add(total);
            } else {
                map[id] = total;
            }
        },

        showMetric: function (id, map) {
            var value = map[id];
            return (map.hasOwnProperty(id) && value > 0);
        },

        valueToString: function (value) {
            return bormat.commaGroups(value);
        }
    },

    emitted: {
        update: function (id, operator, map) {
            var total = new BigInteger(operator.get('tuplesEmittedPSMA'));

            if (map.hasOwnProperty(id)) {
                map[id] = map[id].add(total);
            } else {
                map[id] = total;
            }
        },

        showMetric: function (id, map) {
            var value = map[id];
            return (map.hasOwnProperty(id) && value > 0);
        },

        valueToString: function (value) {
            return bormat.commaGroups(value);
        }
    },

    processedTotal: {
        update: function (id, operator, map) {
            var total = new BigInteger(operator.get('totalTuplesProcessed'));

            if (map.hasOwnProperty(id)) {
                map[id] = map[id].add(total);
            } else {
                map[id] = total;
            }
        },

        showMetric: function (id, map) {
            var value = map[id];
            return (map.hasOwnProperty(id) && value > 0);
        },

        valueToString: function (value) {
            return bormat.commaGroups(value);
        }
    }
}

exports = module.exports = MetricModelFactory;