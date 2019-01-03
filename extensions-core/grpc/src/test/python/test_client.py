#!/usr/bin/env python3

if __name__ == "__main__":
    import grpc.beta.implementations as grpc
    import json


    def serializer(request):
        return json.dumps(request).encode('utf-8')


    def deserializer(response):
        return json.loads(response.decode('utf-8'))


    channel = grpc.insecure_channel('localhost', 53832) #CHANGEME
    stub = grpc.generic_stub(channel, grpc.stub_options(
        request_serializers={("org.apache.druid", "query-json"): serializer},
        response_deserializers={("org.apache.druid", "query-json"): deserializer},
    ))
    query = {"queryType": "topN", "dataSource": {"type": "table", "name": "Test datasource"}, "virtualColumns": [],
             "dimension": {"type": "default", "dimension": "some dimension", "outputName": "some dimension",
                           "outputType": "STRING"}, "metric": {"type": "numeric", "metric": "some metric"},
             "threshold": 1, "intervals": {"type": "LegacySegmentSpec",
                                           "intervals": ["2017-01-01T00:00:00.000Z/2018-01-01T00:00:00.000Z"]},
            "granularity": {"type": "all"}, "aggregations": [{"type": "count", "name": "some metric"}]}

    # Key must be lower case
    md = [("x-druid-query-id", "some_test_id")]
    response = stub.unary_stream("org.apache.druid", "query-json")(query, None, metadata=md)
    expected = {'timestamp': '2017-01-01T00:00:00.000Z', 'result': [{'dim': 1}]}
    # response.cancel()
    for found in response:
        print(found)

    response = stub.unary_stream("org.apache.druid", "query-json")({}, None, metadata=md)
    from grpc.framework.interfaces.face.face import AbortionError
    try:
        for found in response:
            print(found)
        raise RuntimeError("expected an exception from bad query parsing")
    except AbortionError as e:
        # expected
        pass
