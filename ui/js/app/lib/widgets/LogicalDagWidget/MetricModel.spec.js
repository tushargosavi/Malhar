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
var Backbone = require('backbone');
var MetricModel = require('./MetricModel');
var MetricModelFactory = require('./MetricModelFactory');

describe('MetricModel.js', function() {

    beforeEach(function() {
        this.sandbox = sinon.sandbox.create();

        this.operators = new Backbone.Collection();
        this.operators.add(new Backbone.Model({
            logicalName: 'name1',
            latencyMA: 5,
            totalTuplesProcessed: "891936293248"
        }));
        this.operators.add(new Backbone.Model({
            logicalName: 'name1',
            latencyMA: 7,
            totalTuplesProcessed: "891936293248"
        }));
        this.operators.add(new Backbone.Model({
            logicalName: 'name2',
            latencyMA: 10,
            totalTuplesProcessed: "891936293248"
        }));

        this.operators.on('change add remove', function () {
            this.trigger('update');
        });
    });

    it('should have latency metric', function() {
        this.model = MetricModelFactory.getMetricModel('latency', this.operators);
        this.model.subscribe();

        expect(Object.keys(this.model.getMap())).to.have.length(2);
        expect(this.model.getValue('name1')).to.equal(7);

        this.operators.at(0).set('latencyMA', 20);
        expect(this.model.getValue('name1')).to.equal(20);

        var newModel = new Backbone.Model({
            logicalName: 'name2',
            latencyMA: 11
        });
        this.operators.add(newModel);
        expect(Object.keys(this.model.getMap())).to.have.length(2);
        expect(this.model.getValue('name2')).to.equal(11);

        this.operators.remove(newModel);
        expect(Object.keys(this.model.getMap())).to.have.length(2);
        expect(this.model.getValue('name2')).to.equal(10);
    });

    it('should not coud unifiers', function() {
        this.model = MetricModelFactory.getMetricModel('partitions', this.operators);
        this.model.subscribe();

        this.operators.at(0).set('unifierClass', 'UnifierClass');
        expect(this.model.getValue('name1')).to.equal(1);
    });

    it('should show metrics', function() {
        this.model = MetricModelFactory.getMetricModel('partitions', this.operators);
        this.model.subscribe();

        expect(Object.keys(this.model.getMap())).to.have.length(2);
        expect(this.model.showMetric('name1')).to.be.true;
        expect(this.model.showMetric('name2')).to.be.false;
        expect(this.model.showMetric('name3')).to.be.false;
    });

    it('should have partitions metric', function() {
        this.model = MetricModelFactory.getMetricModel('partitions', this.operators);
        this.model.subscribe();

        expect(Object.keys(this.model.getMap())).to.have.length(2);
        expect(this.model.getValue('name1')).to.equal(2);
        expect(this.model.getValue('name2')).to.equal(1);

        this.operators.add(new Backbone.Model({
            logicalName: 'name2'
        }));
        expect(Object.keys(this.model.getMap())).to.have.length(2);
        expect(this.model.getValue('name2')).to.equal(2);
    });

    it('should have processed total metric', function() {
        this.model = MetricModelFactory.getMetricModel('processedTotal', this.operators);
        this.model.subscribe();

        console.log(this.model.getMap()['name1'].toString());
    });

    afterEach(function() {
        this.sandbox.restore();
    });
});