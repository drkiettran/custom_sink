package com.drkiettran.flume.custom_sink;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

/**
 * Hello world!
 *
 */
public class FlumeConsoleOutSink extends AbstractSink implements Configurable {
	private String myProp;

	@Override
	public void configure(Context context) {
		System.out.println("Configuring ...");
		String myProp = context.getString("myProp", "defaultValue");

		// Process the myProp value (e.g. validation)

		// Store myProp for later retrieval by process() method
		this.myProp = myProp;
	}

	@Override
	public void start() {
		// Initialize the connection to the external repository (e.g. HDFS) that
		// this Sink will forward Events to ..
		System.out.println("Starting ...");
	}

	@Override
	public void stop() {
		// Disconnect from the external respository and do any
		// additional cleanup (e.g. releasing resources or nulling-out
		// field values) ..
		System.out.println("Stopping ...");
	}

	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;
		System.out.println("Processing ...");
		// Start transaction
		Channel ch = getChannel();
		Transaction txn = ch.getTransaction();
		txn.begin();
		try {
			// This try clause includes whatever Channel operations you want to do

			Event event = ch.take();
			txn.commit();
			status = Status.READY;
			System.out.println("Body ..." + new String(event.getBody()));
			// Send the Event to the external repository.
			// storeSomeData(e);

		} catch (Throwable t) {
//			txn.rollback();

			// Log exception, handle individual exceptions as needed

			status = Status.BACKOFF;

			// re-throw all Errors
			if (t instanceof Error) {
				throw (Error) t;
			}
		} finally {
			txn.close();

		}
		return status;
	}
}