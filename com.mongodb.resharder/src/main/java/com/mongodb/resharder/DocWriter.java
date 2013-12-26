package com.mongodb.resharder;

import java.util.Stack;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.mongodb.DBObject;

public class DocWriter implements Runnable {
	private final static Queue<DBObject> _queue = new ConcurrentLinkedQueue<DBObject>();
	
	private static AtomicBoolean _running = new AtomicBoolean(false);

	public static boolean get_running() {
		return _running.get();
	}

	public static void push(DBObject obj) {
		_queue.offer(obj);
	}

	int _stopCount = 0;

	public void run() {
		_running.set(true);
		
		try {
			Stack<DBObject> buffer = new Stack<DBObject>();

			// use the same socket for all writes
			// TODO - determine if we need multiple writers to the same collection object
			Conf.get_tgtCollection().getDB().requestStart();
			Conf.get_tgtCollection().getDB().requestEnsureConnection();
			
			MessageLog.push("connected to " + Conf.get_tgtCollection().getDB().getMongo().getConnectPoint(), this.getClass().getSimpleName() + ".");

			while (_running.get()) {
				while (!_queue.isEmpty()) {
					_stopCount = 0;
					// Grab docs of the queue until the buffer is full
					buffer.push(_queue.poll());

					if (buffer.size() == Conf.get_writeBatch()) {
						// Write the docs to the clone Collection and clear the
						// buffer
						// TODO - is there an exception that might need to be
						// handled here?
						Conf.docWrite(buffer);
						buffer.clear();
					}
				}

				try {
					if (++_stopCount > 10) {
						MessageLog.push("Timeout waiting for docs. Shutting down...", this.getClass().getSimpleName());
						Conf.docWrite(buffer);
						buffer.clear();
						shutdown();
					}
					// Wait for more docs
					Thread.sleep(100);
				} catch (InterruptedException ex) {
					Conf.docWrite(buffer);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			MessageLog.push("ERROR: " + e.getMessage(), this.getClass().getSimpleName());
			MessageLog.push("Shutting down clone operation...", this.getClass().getSimpleName());
		} finally {
			// close our connection
			MessageLog.push("disconnected from " + Conf.get_tgtCollection().getDB().getMongo().getConnectPoint(), this.getClass().getSimpleName() + ".");
			Conf.get_tgtCollection().getDB().requestDone();
			shutdown();
		}
	}

	public static void shutdown() {
		_running.set(false);
		CollectionScanner.shutdown();
	}
}
