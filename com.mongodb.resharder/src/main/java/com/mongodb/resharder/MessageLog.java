package com.mongodb.resharder;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public final class MessageLog {
	private static List<String> _messages = new LinkedList<String>();

	public static boolean push(String message, String sender) {
		Date date = new Date();
		DBCollection log = Config.get_log();
		DBObject doc = new BasicDBObject();
		doc.put("ts", date.getTime());
		doc.put("sender", sender);
		doc.put("message", message);

		log.insert(doc);

		synchronized (_messages) {
			_messages.add(date.toString() + ":  (" + sender + ") " + message);
		}

		return true;
	}

	public static String[] getRecentMessages() {
		List<String> messages = new ArrayList<String>();

		synchronized (_messages) {
			while (!_messages.isEmpty())
				messages.add(_messages.remove(0));
		}

		return messages.toArray(new String[0]);
	}
}
