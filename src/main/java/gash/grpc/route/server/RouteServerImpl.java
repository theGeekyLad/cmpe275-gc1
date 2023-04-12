package gash.grpc.route.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import gash.grpc.route.client.RouteClient;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import org.thegeekylad.ServerManager;
import org.thegeekylad.util.constants.MessageType;
import route.Route;
import route.RouteServiceGrpc.RouteServiceImplBase;

/**
 * copyright 2021, gash
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
public class RouteServerImpl extends RouteServiceImplBase {
	private Server svr;

	private ServerManager manager;

	public RouteServerImpl() {
		manager = new ServerManager(this);
	}

	/**
	 * Configuration of the server's identity, port, and role
	 */
	private static Properties getConfiguration(final File path) throws IOException {
		if (!path.exists())
			throw new IOException("missing file");

		Properties rtn = new Properties();
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(path);
			rtn.load(fis);
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					// ignore
				}
			}
		}

		return rtn;
	}

	private static boolean isTypeServer() {
		return Engine.getInstance().serverRole.equals(Engine.ServerType.SERVER.name());
	}

	public static void main(String[] args) throws Exception {
		// TODO check args!

		RouteServerImpl routeServer = new RouteServerImpl();
		routeServer.run(args[0]);
	}

	public void run(String path) throws Exception {
		if (path.endsWith(",")) path = path.substring(0, path.length() - 1);

		try {
			Properties conf = RouteServerImpl.getConfiguration(new File(path));
			Engine.configure(conf);
//			Engine.getConf();

			///////////////////////////////////////////////////////////////////
			// do client stuff if client
//			if (!isTypeServer()) {
//				JSONObject message = new JSONObject(args[1]);
//
//				Route route = Route.newBuilder()
//						.setId(Engine.getInstance().getNextMessageID())
//						.setOrigin(message.getInt("origin"))
//						.setDestination(message.getInt("destination"))
//						.setPath(String.valueOf(Engine.getInstance().getServerPort()))
//						.setPayload(ByteString.copyFrom(message.getString("payload").getBytes()))
//						.build();
//
//				Link link = Engine.getInstance().links.get(0);
//				RouteClient.run(link.getPort(), route);
//
//				Engine.log("Message injected into ring.");
//			}
			///////////////////////////////////////////////////////////////////

			Engine.logDivider();
			Engine.log("" + Engine.getInstance().serverName.toUpperCase());
			Engine.logDivider();
			System.out.println("Initialized.");

			/* Similar to the socket, waiting for a connection */
			while (true) {
				start();

				// new server, send ELC
				manager.sendMessage(manager.getElcMessage(null));

				blockUntilShutdown();
			}

		} catch (IOException e) {
			// TODO better error message
			e.printStackTrace();
		}
	}

	private void start() throws Exception {
		svr = ServerBuilder.forPort(Engine.getInstance().getServerPort()).addService(this).build();

		svr.start();
		Engine.log(String.format("Started."));

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				RouteServerImpl.this.stop();
			}
		});

		Engine.log("Restarting ...");
	}

	public void stop() {
		svr.shutdown();
	}

	private void blockUntilShutdown() throws Exception {
		/* TODO what clean up is required? */
		svr.awaitTermination();

	}

	private boolean verify(route.Route request) {
		return true;
	}

	private Route.Builder getRouteBuilder(Route request) {
		Route.Builder ack = Route.newBuilder();

		ack.setId(request.getId());
		ack.setOrigin(request.getOrigin());
		ack.setDestination(request.getDestination());
		ack.setPath(request.getPath());
		ack.setPayload(request.getPayload());

		return ack;
	}

	/**
	 * server received a message!
	 */
	@Override
	public void request(route.Route request, StreamObserver<route.Route> responseObserver) {
		Engine.log("Incoming message.");

		// TODO refactor to use RouteServer to isolate implementation from transportation

		// TODO use if (verify(request)) for verification

//		 if (verify(request)) {
//			if (isThisMessageForMe(request) && request.getId() != 0) {
//				var w = new Work(request, responseObserver);
//				if (MgmtWorker.isPriority(request))
//					Engine.getInstance().mgmtQueue.add(w);
//				else
//					Engine.getInstance().workQueue.add(w);
//			}

//			if (Engine.logger.isDebugEnabled())
//				Engine.logger.debug("request() qsize = " + Engine.getInstance().workQueue.size());
//		} else {
//			// TODO rejecting the request - what do we do?
//			// buildRejection(ack,request);
//		}

		Route messageAck = manager.getAck(request);

		responseObserver.onNext(messageAck);
		responseObserver.onCompleted();

		// TODO shift responseObserver.onCompleted(); to the bottom if uncommenting below block

//		if (isThisMessageForMe(request)) {
//			Engine.log("The message is for me!");
//
//			// TODO process
//			if (request.getId() == 0) {
//				int finalPort = Integer.parseInt(request.getPath());
////				if (finalPort == Engine.getInstance().getServerID()) {
////					System.out.println(""+ request.getPayload().toStringUtf8());
////					return;
////				}
//				Engine.log(" Payload from processor server: \"" + request.getPayload().toStringUtf8() + "\"");
////				RouteClient.run(finalPort, request);
//				return;  // this is the END !!!!!!!
//			}
//
//			Route.Builder replyBuilder = getRouteBuilder(request);
//			replyBuilder.setId(0);  // crucial
//			replyBuilder.setOrigin(request.getDestination());
//			replyBuilder.setDestination(request.getOrigin());
//			replyBuilder.setPayload(ByteString.copyFrom("Got it man.".getBytes()));
//
//			request = replyBuilder.build();
////			prepareAndSendReply(replyBuilder.build());
//		}
//
//		// this is a reply message on the way back
//		else if (request.getId() == 0) {
//			Engine.log("This is a reply. Forwarding the message ...");
////			prepareAndSendReply(request);
//		}
//
//		// forwarding message ahead in the ring
//		else {
//			Engine.log("Forwarding the message ...");
//		}

//		request = getRouteBuilder(request).setPath(request.getPath() + "/" + Engine.getInstance().getServerPort()).build();

		manager.processIncomingMessage(request);
	}

	private boolean isThisMessageForMe(Route request) {
		return Engine.getInstance().getServerID() == request.getDestination();
	}

	private void prepareAndSendReply(Route request) {
		String[] pathParts = request.getPath().split("/");
		if (request.getPath().contains("/"))
			request = getRouteBuilder(request).setPath(request.getPath().substring(0, request.getPath().lastIndexOf("/"))).build();
		else
			request = getRouteBuilder(request).setPath("").build();

		Engine.log(request.getPath());
		Engine.log("pathParts[pathParts.length - 1]");
		Engine.log(pathParts[pathParts.length - 1]);

		RouteClient.run(Integer.parseInt(pathParts[pathParts.length - 1]), request);
	}
}
