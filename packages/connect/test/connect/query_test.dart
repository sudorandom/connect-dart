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

import 'dart:convert';
import 'dart:typed_data';

import 'package:connectrpc/connect.dart';
import 'package:connectrpc/protobuf.dart';
import 'package:connectrpc/protocol/connect.dart' as connect;
import 'package:connectrpc/src/connect/stable_codec.dart';
import 'package:protobuf/well_known_types/google/protobuf/wrappers.pb.dart';
import 'package:test/test.dart';

const _safe = Spec(
  '/TestService/Safe',
  StreamType.unary,
  StringValue.new,
  StringValue.new,
  idempotency: Idempotency.noSideEffects,
);
const _idempotent = Spec(
  '/TestService/Idempotent',
  StreamType.unary,
  StringValue.new,
  StringValue.new,
  idempotency: Idempotency.idempotent,
);
const _unknown = Spec(
  '/TestService/Unknown',
  StreamType.unary,
  StringValue.new,
  StringValue.new,
);
const _serverStream = Spec(
  '/TestService/ServerStream',
  StreamType.server,
  StringValue.new,
  StringValue.new,
  idempotency: Idempotency.noSideEffects,
);

/// A [Compression] that reverses the bytes, to observe that it was applied.
final class _ReverseCompression implements Compression {
  const _ReverseCompression();

  @override
  String get name => 'reverse';

  @override
  Uint8List encode(Uint8List input) =>
      Uint8List.fromList(input.reversed.toList());

  @override
  Uint8List decode(Uint8List encoded) => encode(encoded);

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

/// A codec that is not a [StableCodec], and so can never use GET.
final class _UnstableCodec implements Codec {
  final _inner = const ProtoCodec();

  @override
  String get name => _inner.name;

  @override
  Uint8List encode(Object message) => _inner.encode(message);

  @override
  void decode(Uint8List data, Object message) => _inner.decode(data, message);
}

/// A [StableCodec] that records which encoding was requested.
final class _RecordingCodec implements StableCodec {
  final _inner = const ProtoCodec();
  int encodeCalls = 0;
  int stableEncodeCalls = 0;

  @override
  String get name => _inner.name;

  @override
  bool get isBinary => true;

  @override
  Uint8List encode(Object message) {
    encodeCalls++;
    return _inner.encode(message);
  }

  @override
  Uint8List stableEncode(Object message) {
    stableEncodeCalls++;
    return _inner.stableEncode(message);
  }

  @override
  void decode(Uint8List data, Object message) => _inner.decode(data, message);
}

final class _Sent {
  final HttpRequest request;
  final Uint8List? body;
  _Sent(this.request, this.body);
}

/// Returns an [HttpClient] that records requests and replies with [status].
HttpClient _recorder(
  List<_Sent> sent, {
  int status = 200,
  String contentType = 'application/proto',
  List<int>? responseBody,
}) {
  return (req) async {
    final chunks = <int>[];
    if (req.body case final body?) {
      await for (final chunk in body) {
        chunks.addAll(chunk);
      }
    }
    sent.add(_Sent(req, req.body == null ? null : Uint8List.fromList(chunks)));
    final header = Headers();
    final Stream<Uint8List> body;
    if (status == 200) {
      header['content-type'] = contentType;
      body = Stream.value(
        Uint8List.fromList(
          responseBody ?? StringValue(value: 'pong').writeToBuffer(),
        ),
      );
    } else {
      body = const Stream.empty();
    }
    return HttpResponse(status, header, body, Headers());
  };
}

void main() {
  group('Connect transport useHttpQuery', () {
    final request = StringValue(value: 'ping');
    final encoded = request.writeToBuffer();

    connect.Transport newTransport(
      List<_Sent> sent, {
      bool useHttpGet = false,
      bool useHttpQuery = false,
      Codec codec = const ProtoCodec(),
      Compression? sendCompression,
      int status = 200,
      String contentType = 'application/proto',
      List<int>? responseBody,
    }) {
      return connect.Transport(
        baseUrl: 'https://example.com',
        codec: codec,
        httpClient: _recorder(
          sent,
          status: status,
          contentType: contentType,
          responseBody: responseBody,
        ),
        useHttpGet: useHttpGet,
        useHttpQuery: useHttpQuery,
        sendCompression: sendCompression,
        acceptCompressions: [if (sendCompression != null) sendCompression],
      );
    }

    test('uses POST by default', () async {
      final sent = <_Sent>[];
      await newTransport(sent).unary(_safe, request);
      expect(sent.single.request.method, equals('POST'));
    });

    test('sends QUERY with the same url, headers and body as POST', () async {
      final post = <_Sent>[];
      await newTransport(post).unary(_safe, request);
      final query = <_Sent>[];
      final res = await newTransport(
        query,
        useHttpQuery: true,
      ).unary(_safe, request);
      expect(res.message.value, equals('pong'));
      final sentQuery = query.single;
      final sentPost = post.single;
      expect(sentQuery.request.method, equals('QUERY'));
      expect(
        sentQuery.request.url,
        equals('https://example.com/TestService/Safe'),
      );
      expect(sentQuery.request.url, equals(sentPost.request.url));
      expect(sentQuery.body, equals(encoded));
      expect(sentQuery.body, equals(sentPost.body));
      expect(
        sentQuery.request.header.entries.toList(),
        equals(sentPost.request.header.entries.toList()),
      );
      expect(sentQuery.request.header['content-type'], 'application/proto');
      expect(sentQuery.request.header['connect-protocol-version'], '1');
    });

    test('sends QUERY with the JSON codec', () async {
      final sent = <_Sent>[];
      final transport = newTransport(
        sent,
        useHttpQuery: true,
        codec: const JsonCodec(),
        contentType: 'application/json',
        responseBody: utf8.encode('"pong"'),
      );
      final res = await transport.unary(_safe, request);
      expect(res.message.value, equals('pong'));
      final only = sent.single;
      expect(only.request.method, equals('QUERY'));
      expect(only.request.header['content-type'], 'application/json');
      expect(utf8.decode(only.body!), equals('"ping"'));
      expect(only.request.url, isNot(contains('?')));
    });

    test('uses POST for ineligible unary calls', () async {
      final sent = <_Sent>[];
      final transport = newTransport(sent, useHttpQuery: true);
      await transport.unary(_idempotent, request);
      await transport.unary(_unknown, request);
      expect(sent.map((s) => s.request.method), equals(['POST', 'POST']));
    });

    test('uses POST for streaming calls', () async {
      final sent = <_Sent>[];
      final transport = newTransport(sent, useHttpQuery: true);
      // The stub replies with a unary content type, which a stream rejects.
      // Only the request that was sent matters here.
      await expectLater(
        transport.stream(_serverStream, Stream.value(request)),
        throwsA(isA<ConnectException>()),
      );
      expect(sent.single.request.method, equals('POST'));
    });

    test('uses stable encoding', () async {
      final codec = _RecordingCodec();
      await newTransport(
        <_Sent>[],
        useHttpQuery: true,
        codec: codec,
      ).unary(_safe, request);
      expect(codec.stableEncodeCalls, equals(1));
      expect(codec.encodeCalls, equals(0));
    });

    test('GET still wins when both are enabled', () async {
      final sent = <_Sent>[];
      await newTransport(
        sent,
        useHttpGet: true,
        useHttpQuery: true,
      ).unary(_safe, request);
      expect(sent.single.request.method, equals('GET'));
      expect(sent.single.body, isNull);
    });

    test('QUERY replaces POST when GET cannot be used', () async {
      final sent = <_Sent>[];
      final transport = newTransport(
        sent,
        useHttpGet: true,
        useHttpQuery: true,
        codec: _UnstableCodec(),
      );
      await transport.unary(_safe, request);
      await transport.unary(_idempotent, request);
      expect(sent.map((s) => s.request.method), equals(['QUERY', 'POST']));
    });

    test('compresses the request', () async {
      final sent = <_Sent>[];
      await newTransport(
        sent,
        useHttpQuery: true,
        sendCompression: const _ReverseCompression(),
      ).unary(_safe, request);
      final only = sent.single;
      expect(only.request.method, equals('QUERY'));
      expect(only.request.header['content-encoding'], 'reverse');
      expect(only.body, equals(encoded.reversed.toList()));
    });

    test('surfaces 405 without retrying with POST', () async {
      final sent = <_Sent>[];
      final transport = newTransport(sent, useHttpQuery: true, status: 405);
      await expectLater(
        transport.unary(_safe, request),
        throwsA(
          isA<ConnectException>().having(
            (e) => e.message,
            'message',
            contains('405'),
          ),
        ),
      );
      expect(sent.map((s) => s.request.method), equals(['QUERY']));
    });
  });
}
