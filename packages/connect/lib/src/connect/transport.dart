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

import "../codec.dart";
import "../compression.dart";
import "../http.dart";
import "../interceptor.dart";
import "../protocol/transport.dart";
import "protocol.dart";

/// Transport for the Connect protocol.
///
/// If [useHttpGet] is true, unary RPCs that are side-effect-free are sent with
/// the GET method.
///
/// If [useHttpQuery] is true, unary RPCs that are side-effect-free are sent
/// with the HTTP QUERY method (RFC 10008) instead of POST. The request is
/// identical to a POST request except for the method. QUERY replaces POST, not
/// GET: when both options are enabled, requests that can be sent with GET still
/// are. A server or intermediary that rejects QUERY surfaces an error, the
/// request is not retried with POST.
final class Transport extends ProtocolTransport {
  Transport({
    required String baseUrl,
    required Codec codec,
    required HttpClient httpClient,
    List<Interceptor>? interceptors,
    bool useHttpGet = false,
    bool useHttpQuery = false,
    Compression? sendCompression,
    List<Compression>? acceptCompressions,
  }) : super(
         baseUrl,
         codec,
         Protocol(useHttpGet, useHttpQuery: useHttpQuery),
         httpClient,
         interceptors ?? [],
         sendCompression,
         acceptCompressions ?? [],
       );
}
