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

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:connectrpc/connect.dart';
import 'package:connectrpc/http2.dart' as http2c;
import 'package:http2/http2.dart' as http2;
import 'package:test/test.dart';

void main() {
  test('HTTP/2 client sends the QUERY method with a body', () async {
    final serverSocket = await ServerSocket.bind(
      InternetAddress.loopbackIPv4,
      0,
    );
    addTearDown(() => serverSocket.close());
    final methods = <String>[];
    final bodies = <String>[];
    serverSocket.listen((socket) {
      final conn = http2.ServerTransportConnection.viaSocket(socket);
      conn.incomingStreams.listen((stream) async {
        final body = <int>[];
        await for (final message in stream.incomingMessages) {
          switch (message) {
            case http2.HeadersStreamMessage():
              for (final header in message.headers) {
                if (utf8.decode(header.name) == ':method') {
                  methods.add(utf8.decode(header.value));
                }
              }
            case http2.DataStreamMessage():
              body.addAll(message.bytes);
          }
        }
        bodies.add(utf8.decode(body));
        stream.sendHeaders([
          http2.Header.ascii(':status', '200'),
          http2.Header.ascii('content-type', 'application/json'),
        ]);
        stream.sendData(utf8.encode('"pong"'), endStream: true);
      });
    });
    final transport = http2c.Http2ClientTransport();
    final client = http2c.createHttpClient(transport: transport);
    final res = await client(
      HttpRequest(
        'http://${serverSocket.address.host}:${serverSocket.port}/Svc/Safe',
        'QUERY',
        Headers()..['content-type'] = 'application/json',
        Stream.value(Uint8List.fromList(utf8.encode('"ping"'))),
        null,
      ),
    );
    final responseBody = <int>[];
    await for (final chunk in res.body) {
      responseBody.addAll(chunk);
    }
    expect(res.status, equals(200));
    expect(utf8.decode(responseBody), equals('"pong"'));
    expect(methods, equals(['QUERY']));
    expect(bodies, equals(['"ping"']));
  });
}
