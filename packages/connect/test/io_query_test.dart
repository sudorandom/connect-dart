// Copyright 2024-2025 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

@TestOn('!js')
library;

import 'dart:convert';
import 'dart:io' as io;

import 'package:connectrpc/connect.dart';
import 'package:connectrpc/io.dart' as http1;
import 'package:connectrpc/protobuf.dart';
import 'package:connectrpc/protocol/connect.dart' as connect;
import 'package:protobuf/well_known_types/google/protobuf/wrappers.pb.dart';
import 'package:test/test.dart';

void main() {
  test('dart:io client sends the QUERY method with a body', () async {
    final server = await io.HttpServer.bind(io.InternetAddress.loopbackIPv4, 0);
    addTearDown(() => server.close(force: true));
    final seen = <({String method, String body, String? contentType})>[];
    server.listen((req) async {
      final body = await utf8.decodeStream(req);
      seen.add((
        method: req.method,
        body: body,
        contentType: req.headers.contentType?.mimeType,
      ));
      req.response.headers.contentType = io.ContentType('application', 'json');
      req.response.write('"pong"');
      await req.response.close();
    });
    final transport = connect.Transport(
      baseUrl: 'http://localhost:${server.port}',
      codec: const JsonCodec(),
      httpClient: http1.createHttpClient(io.HttpClient()),
      useHttpQuery: true,
    );
    final res = await transport.unary(
      const Spec(
        '/TestService/Safe',
        StreamType.unary,
        StringValue.new,
        StringValue.new,
        idempotency: Idempotency.noSideEffects,
      ),
      StringValue(value: 'ping'),
    );
    expect(res.message.value, equals('pong'));
    expect(seen.single.method, equals('QUERY'));
    expect(seen.single.body, equals('"ping"'));
    expect(seen.single.contentType, equals('application/json'));
  });
}
