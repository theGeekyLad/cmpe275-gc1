package gash.grpc.route.client;

import com.google.protobuf.ByteString;

import gash.grpc.route.server.Engine;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import route.Route;
import route.RouteServiceGrpc;

import java.util.concurrent.TimeUnit;

/**
 * copyright 2023, gash
 *
 * Gash licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

public class RouteClient {
	// when using server.conf
	private static long clientID = 501;
	private static int port = 3036;

	// private static long clientID = 100;
	// private static int port = 2100;

	private static final Route constructMessage(int mID, int toID, String path, String payload) {
		Route.Builder bld = Route.newBuilder();
		bld.setId(mID);
		bld.setDestination(toID);
		bld.setOrigin(RouteClient.clientID);
		bld.setPath(path);

		byte[] hello = payload.getBytes();
		bld.setPayload(ByteString.copyFrom(hello));

		return bld.build();
	}

	private static final void response(Route reply) {
		// TODO handle the reply/response from the server
		var payload = new String(reply.getPayload().toByteArray());
		System.out.println("reply: " + reply.getId() + ", from: " + reply.getOrigin() + ", payload: " + payload);
	}

//	public static void main(String[] args) {
//		RouteClient.run(RouteClient.port, 3038, 1);
//	}

//	public static void run(int linkPort, int destinationId, int I) {
//		ManagedChannel ch = ManagedChannelBuilder.forAddress("localhost", linkPort).usePlaintext().build();
//		RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
//
//		for (int i = 0; i < I; i++) {
//			// simulate different type of messages that can be sent
//			var path = (i % 5 == 0) ? "/manage/something" : "/to/somewhere";
//			var msg = RouteClient.constructMessage(i, destinationId, path, "hello " + i);
//
//			// blocking!
//			var r = stub.request(msg);
//			response(r);
//		}
//
//		ch.shutdown();
//	}

	public static void run(int linkPort, Route request) {
		ManagedChannel ch = ManagedChannelBuilder.forAddress("localhost", linkPort).usePlaintext().build();
		RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);

		// blocking!
		var r = stub.request(request);

		response(r);

		ch.shutdown();
	}
}
