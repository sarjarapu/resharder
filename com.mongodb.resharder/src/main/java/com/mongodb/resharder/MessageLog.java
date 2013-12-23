package com.mongodb.resharder;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public final class MessageLog {
	private static List<String> _messages = new LinkedList<String>();
	private static long _last = new Date().getTime();

	public static boolean push(String message, String sender) {
		Date date = new Date();
		DBObject doc = new BasicDBObject();
		doc.put("ts", date.getTime());
		doc.put("sender", sender);
		doc.put("message", message);

		Config.get_log().insert(doc);

		return true;
	}

	public static String[] getRecentMessages() {
		List<String> messages = new ArrayList<String>();

		if (Config.isInitialized()) {
			DBCursor cursor = Config.get_log().find(new BasicDBObject("ts", new BasicDBObject("$gt", _last)))
					.sort(new BasicDBObject("ts", 1));

			while (cursor.hasNext()) {
				BasicDBObject doc = (BasicDBObject) cursor.next();
				Date date = new Date();
				_last = doc.getLong("ts");
				date.setTime(_last);
				messages.add(date.toString() + ": (" + doc.getString("sender") + ") " + doc.getString("message"));
			}
		}

		return messages.toArray(new String[0]);
	}
}
