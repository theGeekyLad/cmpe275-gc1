package gash.grpc.route.server;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker extends Thread {
	protected static Logger logger = LoggerFactory.getLogger("worker");
	private boolean forever = true;

	public Worker() {
	}

	public void shutdown() {
		logger.info("shutting down worker " + this.getId());
		forever = true;
	}

	private void doWork(Work w) {

		if (w == null)
			return;

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

	@Override
	public void run() {
		// TODO not a good idea to spin on work --> wastes CPU cycles
		while (forever) {
			try {
				if (logger.isDebugEnabled())
					logger.debug("run() work qsize = " + Engine.getInstance().workQueue.size());
				
				var w = Engine.getInstance().workQueue.poll(2, TimeUnit.SECONDS);
				doWork(w);
			} catch (Exception e) {
				logger.error("worker failure",e);
			}
		}

		// TODO any shutdown steps
	}
}
