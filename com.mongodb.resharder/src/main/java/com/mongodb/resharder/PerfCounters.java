package com.mongodb.resharder;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class PerfCounters implements Runnable {
	private static AtomicBoolean _running = new AtomicBoolean(false);
	private static DBObject _rates = new BasicDBObject("docs", 0).append("oplogs", 0).append("orphans", 0);

	private long _docs, _oplogs, _orphans, _ts = System.currentTimeMillis();

	public static void shutdown() {
		_running.set(false);
	}

	public static String getRateCounters() {
		return _rates.toString();
	}

	@Override
	public void run() {
		this._running.set(true);

		while (_running.get()) {
			long docs, oplogs, orphans, now = System.currentTimeMillis(), secs = (now - _ts) / 1000;

			if (secs == 0)
				secs = 1;

			Map<String, Long> counters = Conf.getCounters();

			docs = counters.get("docCount").longValue();
			orphans = counters.get("orphanCount").longValue();
			oplogs = counters.get("oplogCount").longValue();

			_rates.put("docs", ((docs - _docs) / secs));
			_rates.put("oplogs", ((oplogs - _oplogs) / secs));
			_rates.put("orphans", ((orphans - _orphans) / secs));

			_docs = docs;
			_oplogs = oplogs;
			_orphans = orphans;

			_ts = now;
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// NOOP
			}
		}
	}

}
