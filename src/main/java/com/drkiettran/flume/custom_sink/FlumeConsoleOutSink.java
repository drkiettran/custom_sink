package com.drkiettran.flume.custom_sink;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlumeConsoleOutSink extends AbstractSink implements Configurable {
	private static Logger logger = LoggerFactory.getLogger(FlumeConsoleOutSink.class);

	@Override
	public void configure(Context context) {
		logger.info("Configuring ...");
	}

	@Override
	public void start() {
		// Initialize the connection to the external repository (e.g. HDFS) that
		// this Sink will forward Events to ..
		logger.info("Starting ...");
	}

	@Override
	public void stop() {
		// Disconnect from the external respository and do any
		// additional cleanup (e.g. releasing resources or nulling-out
		// field values) ..
		logger.info("Stopping ...");
	}

	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;
		logger.info("Processing ...");
		// Start transaction
		Channel ch = getChannel();
		Transaction txn = ch.getTransaction();
		txn.begin();
		try {
			// This try clause includes whatever Channel operations you want to do

			Event event = ch.take();
			txn.commit();
			status = Status.READY;
			logger.info("Body ... {}", new String(event.getBody()));

		} catch (Throwable t) {
			status = Status.BACKOFF;

			if (t instanceof Error) {
				throw (Error) t;
			}
		} finally {
			txn.close();

		}
		return status;
	}
}