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
/**
 * Info widget for port instances.
 */
var _ = require('underscore');
var Backbone = require('backbone');
var kt = require('knights-templar');
var bormat = require('bormat');
var BaseView = DT.widgets.Widget;
var d3 = require('d3');
var dagreD3 = require('dagre-d3');
var MetricModel = require('./MetricModel');
var MetricModelFactory = require('./MetricModelFactory');

var LogicalDagWidget = BaseView.extend({

    events: {
        'change .metric-select': 'changeMetric',
        'click .metric-prev': 'prevMetric',
        'click .metric-next': 'nextMetric'
    },

    initialize: function(options) {
        BaseView.prototype.initialize.call(this, options);

        this.operators = options.operators;

        this.metrics = [
            {
                value: 'processed',
                label: 'Processed/sec'
            },
            {
                value: 'emitted',
                label: 'Emitted/sec'
            },
            {
                value: 'latency',
                label: 'Max Latency (ms)'
            },
            {
                value: 'partitions',
                label: 'Partition Count'
            },
            {
                value: 'containers',
                label: 'Container Count'
            },
            {
                value: 'avgCpu',
                label: 'Average CPU (%)'
            },
            {
                value: 'processedTotal',
                label: 'Processed Total'
            }

        ];
        this.metricIds = _.map(this.metrics, function (metric) {
            return metric.value;
        });

        this.model.loadLogicalPlan({
            success: _.bind(function(data) {
                this.displayGraph(data.toJSON());

                this.metricModel = new MetricModel(null, { operators: this.operators });
                this.partitionsMetricModel = MetricModelFactory.getMetricModel('partitions', this.operators);
                this.listenTo(this.partitionsMetricModel, 'change', this.updatePartitions);
                this.partitionsMetricModel.subscribe();

                this.metricModel = MetricModelFactory.getMetricModel(this.metricIds[0], this.operators);
                this.listenTo(this.metricModel, 'change', this.update);
                this.metricModel.subscribe();
            }, this)
        });
    },

    template: kt.make(__dirname+'/LogicalDagWidget.html','_'),

    html: function() {
        return this.template({ metrics: this.metrics });
    },

    displayGraph: function(data, physicalPlan) {
        var graph = this.buildGraph(data);
        this.renderGraph(graph, this.$el.find('.app-dag > svg')[0]);
    },

    buildGraph: function(data) {
        var nodes = [];

        _.each(data.operators, function(value, key) {
            var node = { id: value.name, value: { label: value.name } };
            nodes.push(node);
        });

        var links = [];

        _.each(data.streams, function(stream, key) {
            var source = stream.source.operatorName;
            _.each(stream.sinks, function(sink) {
                var target = sink.operatorName;
                var link = { u: source, v: target, value: { label: stream.name } };
                links.push(link);
            });
        });

        var graph = { nodes: nodes, links: links };
        return graph;
    },

    changeMetric: function () {
        var selMetric = this.$('.metric-select').val();

        if (this.operators && this.metricModel) {
            this.metricModel.unsubscribe();

            this.metricModel = MetricModelFactory.getMetricModel(selMetric, this.operators);
            this.listenTo(this.metricModel, 'change', this.update);
            this.metricModel.subscribe();
        }
    },

    prevMetric: function (event) {
        event.preventDefault();
        var selMetric = this.$('.metric-select').val();
        var index = _.indexOf(this.metricIds, selMetric);
        var nextIndex = (this.metricIds.length + index - 1) % this.metricIds.length;
        this.$('.metric-select').val(this.metricIds[nextIndex]);
        this.changeMetric();
    },

    nextMetric: function (event) {
        event.preventDefault();
        var selMetric = this.$('.metric-select').val();
        var index = _.indexOf(this.metricIds, selMetric);
        var nextIndex = (index + 1) % this.metricIds.length;
        this.$('.metric-select').val(this.metricIds[nextIndex]);
        this.changeMetric();
    },

    update: function () {
        this.updateMetricLabels(this.graph, this.metricModel);
    },

    updatePartitions: function () {
        var that = this;
        this.svgNodes.each(function (d, i) {
            var nodeSvg = d3.select(this);

            var multiple = that.partitionsMetricModel.showMetric(d);

            var filter = multiple ? 'url(#f1)' : null;
            //var nodeLabel = nodeSvg.select('.label');
            //nodeLabel.attr('filter', filter);

            var nodeRect = nodeSvg.select('.label > rect');
            nodeRect.attr('filter', filter);
        });
    },

    updateMetricLabels: function (graph, metric) {
        this.svgNodes.each(function (d, i) {
            var nodeSvg = d3.select(this);

            var value = metric.getTextValue(d);
            var showMetric = metric.showMetric(d);

            var metricLabel = nodeSvg.select('.node-metric-label');
            var metricLabelText = metricLabel.select('tspan');

            var text = showMetric ? value : '';
            metricLabelText.text(text);

            var bbox = metricLabel.node().getBBox();
            var h = graph.node(d).height;
            metricLabel.attr("transform",
                "translate(" + (-bbox.width / 2) + "," + (-bbox.height - h / 2 - 4) + ")");
        });
    },

    postRender: function (graph, root) {
        // add metric label, structure is the following
        // g.node
        // g.node-metric-label
        //   text
        //     tspan

        this.graph = graph;
        this.svgNodes = root.selectAll("g .node");

        this.svgNodes.each(function (d, i) {
            var nodeSvg = d3.select(this);
            var labelSvg = nodeSvg.append("g").attr('class', 'node-metric-label');

            labelSvg
                .append("text")
                .attr("text-anchor", "left")
                .append("tspan")
                .attr("dy", "1em")
                .text(function() { return ''; });

            var bbox = labelSvg.node().getBBox();

            var h = graph.node(d).height;

            labelSvg.attr("transform",
                "translate(" + (-bbox.width / 2) + "," + (-bbox.height - h / 2 - 4) + ")");
        });

        //TODO
        if (false)
        setInterval(function () {
            root.selectAll("g .node-metric-label").each(function (d, i) {
                var sec = Math.floor(Date.now()/1000)%10;
                var base;
                var m = Math.floor(sec/3);
                if (m === 0) {
                    base = 100;
                } else if (m === 1) {
                    base = 10000;
                } else {
                    base = 1000000;
                }
                var text = 'm' + m + ' ' + Math.round(base + Math.random() * base);
                var labelG = d3.select(this);
                var tspan = labelG.select('tspan');
                tspan.text(text);

                var bbox = labelG.node().getBBox();
                var h = graph.node(d).height;

                labelG.attr("transform",
                    "translate(" + (-bbox.width / 2) + "," + (-bbox.height - h / 2 - 3) + ")");
            });
        }, 250);
    },

    renderGraph: function(graph, selector) {
        var svgParent = jQuery(selector);
        var nodes = graph.nodes;
        var links = graph.links;

        var graphElem = svgParent.children('g').get(0);
        var svg = d3.select(graphElem);
        svg.selectAll("*").remove();

        var renderer = new dagreD3.Renderer();

        var oldPostRender = renderer._postRender;
        renderer._postRender = function (graph, root) {
            oldPostRender.call(renderer, graph, root);
            this.postRender(graph, root);
        }.bind(this);

        var layout = dagreD3.layout().rankDir('LR');
        renderer.layout(layout).run(dagreD3.json.decode(nodes, links), svg.append("g"));

        // TODO
        // Adjusting height to content
        var main = svgParent.find('g > g');
        var h = main.get(0).getBoundingClientRect().height;
        var newHeight = h + 40;
        newHeight = newHeight < 80 ? 80 : newHeight;
        newHeight = newHeight > 500 ? 500 : newHeight;
        svgParent.height(newHeight);

        // Zoom
        d3.select(svgParent.get(0)).call(d3.behavior.zoom().on("zoom", function() {
            var ev = d3.event;
            svg.select("g")
                .attr("transform", "translate(" + ev.translate + ") scale(" + ev.scale + ")");
        }));
    }

});

exports = module.exports = LogicalDagWidget