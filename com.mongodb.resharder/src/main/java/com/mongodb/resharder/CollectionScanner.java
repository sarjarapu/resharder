/**
 * 
 */
package com.mongodb.resharder;

import java.util.concurrent.atomic.AtomicBoolean;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class CollectionScanner implements Runnable {
	private DBCollection _source = null;
	private static AtomicBoolean _running = new AtomicBoolean(false);
	private int _numread = 0;
	private int _batch = 0;

	public CollectionScanner(DBCollection source, int batch) {
		this._source = source;
		this._batch = batch;
	}

	public void run() {
		_running.set(true);

		// use the same socket for all reads
		try {
			_source.getDB().requestStart();
			_source.getDB().requestEnsureConnection();
			MessageLog.push("Reader started.", this.getClass().getSimpleName());

			while (_running.get()) {
				DBCursor cursor = _source.find().sort(new BasicDBObject("$natural", 1)).skip(_numread).limit(_batch);
				try {
				
					while (cursor.hasNext()) {
						// Put docs on queue
						DocWriter.push(cursor.next());
						_numread++;
					}
				} finally {
					// close the cursor and socket
					cursor.close();
				}
			}
		} finally {
			_source.getDB().requestDone();
			MessageLog.push("Reader stopped.", this.getClass().getSimpleName());
		}
	}

	public static boolean get_running() {
		return _running.get();
	}

	public static void set_running(boolean running) {
		_running.set(running);
	}
}
