import {Datasource} from "../module";
import Q from "q";

describe('GenericDatasource', function() {
    var ctx = {};

    beforeEach(function() {
        ctx.$q = Q;
        ctx.backendSrv = {};
        ctx.templateSrv = {};
        ctx.ds = new Datasource({}, ctx.$q, ctx.backendSrv, ctx.templateSrv);
    });

    it('should return an empty array when no targets are set', function(done) {
        ctx.ds.query({targets: []}).then(function(result) {
            expect(result.data).to.have.length(0);
            done();
        });
    });

    it('should return the server results when a target is set', function(done) {
        ctx.backendSrv.datasourceRequest = function(request) {
            return ctx.$q.when({
                _request: request,
                data: [
                    {
                        target: 'X',
                        datapoints: [1, 2, 3]
                    }
                ]
            });
        };

        ctx.templateSrv.replace = function(data) {
            return data;
        }

        ctx.ds.query({targets: ['hits']}).then(function(result) {
            expect(result.data).to.have.length(1);
            var series = result.data[0];
            expect(series.target).to.equal('X');
            expect(series.datapoints).to.have.length(3);
            done();
        });
    });

    it ('should return empty metric result list, no search atm.', function(done) {
        ctx.backendSrv.datasourceRequest = function(request) {
            return ctx.$q.when({
                _request: request,
                data: [
                    "metric_0",
                    "metric_1",
                    "metric_2",
                ]
            });
        };

        ctx.templateSrv.replace = function(data) {
            return data;
        }

        ctx.ds.metricFindQuery({target: null}).then(function(result) {
            expect(result).to.have.length(0);
            done();
        });
    });

    it('should support tag keys', function(done) {
        var data =  [{'type': 'string', 'text': 'One', 'key': 'one'}, {'type': 'string', 'text': 'two', 'key': 'Two'}];

        ctx.backendSrv.datasourceRequest = function(request) {
            return ctx.$q.when({
                _request: request,
                data: data
            });
        };

        ctx.ds.getTagKeys().then(function(result) {
            expect(result).to.have.length(2);
            expect(result[0].type).to.equal(data[0].type);
            expect(result[0].text).to.equal(data[0].text);
            expect(result[0].key).to.equal(data[0].key);
            expect(result[1].type).to.equal(data[1].type);
            expect(result[1].text).to.equal(data[1].text);
            expect(result[1].key).to.equal(data[1].key);
            done();
        });
    });

    it('should support tag values', function(done) {
        var data =  [{'key': 'eins', 'text': 'Eins!'}, {'key': 'zwei', 'text': 'Zwei'}, {'key': 'drei', 'text': 'Drei!'}];

        ctx.backendSrv.datasourceRequest = function(request) {
            return ctx.$q.when({
                _request: request,
                data: data
            });
        };

        ctx.ds.getTagValues().then(function(result) {
            expect(result).to.have.length(3);
            expect(result[0].text).to.equal(data[0].text);
            expect(result[0].key).to.equal(data[0].key);
            expect(result[1].text).to.equal(data[1].text);
            expect(result[1].key).to.equal(data[1].key);
            expect(result[2].text).to.equal(data[2].text);
            expect(result[2].key).to.equal(data[2].key);
            done();
        });
    });

});
