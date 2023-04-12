package gash.grpc.route.server;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MgmtWorker extends Thread {
	protected static Logger logger = LoggerFactory.getLogger("manager");
	private boolean forever = true;

	// paths that are processed by this worker
	private static String[] paths = { "/manage/", "/election/" };

	public static boolean isPriority(route.Route request) {
		var rtn = false;

		if (request != null) {
			// TODO how do you speed this testing up?
			for (String t : paths) {
				if (request.getPath().toLowerCase().startsWith(t)) {
					rtn = true;
					break;
				}
			}
		}

		return rtn;
	}

	public MgmtWorker() {
	}

	public void shutdown() {
		logger.info("shutting down manager");
		forever = true;
	}

	private void doWork(Work w) {

		if (w != null) {
			if (w.request.getDestination() == Engine.getInstance().serverID) {
				try {

					logger.info("*** doWork() " + w + " ***");

					// simulates latency
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				// forward the message to Link instances
				for (var link : Engine.getInstance().links) {
					if (link.getServerID() == w.request.getDestination()) {
						logger.info("TODO: send message");
					}
				}

				// if no direct destination exists, forward to all
				// links or the next link?
			}
		}
	}

	@Override
	public void run() {
		// TODO not a good idea to spin on work --> wastes CPU cycles
		while (forever) {
			try {
				if (logger.isDebugEnabled())
					logger.debug("run() mgmt qsize = " + Engine.getInstance().mgmtQueue.size());

				var w = Engine.getInstance().mgmtQueue.poll(2, TimeUnit.SECONDS);

				if (w != null)
					doWork(w);

			} catch (Exception e) {
				logger.error("manager failure",e);
			}
		}
	}
}
