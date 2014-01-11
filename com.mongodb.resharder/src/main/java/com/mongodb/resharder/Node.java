package com.mongodb.resharder;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class Node {
	private BasicDBObject _doc;

	public Node(String name, String host, int x, int y) {
		_doc = new BasicDBObject();
		_doc.put("name", name);
		_doc.put("host", host);
		_doc.put("state", "");
		_doc.put("x", x);
		_doc.put("y", y);

		_doc.put("connections", new String[0]);

		save();
	}

	public Node(DBObject doc) {
		_doc = (BasicDBObject) doc;
	}

	private int getY() {
		return _doc.getInt("y");
	}

	private int getX() {
		return _doc.getInt("x");
	}

	public void save() {
		Config.get_nodes().save(_doc);
	}

	private void refresh() {
		_doc = (BasicDBObject) Config.get_nodes().findOne(new BasicDBObject("_id", _doc.getObjectId("_id")));
	}

	public String getName() {
		return _doc.getString("name");
	}

	public String getHost() {
		return _doc.getString("host");
	}

	public String getState() {
		return _doc.getString("state");
	}

	public void setState(String state) {
		_doc.put("state", state);

		save();
	}

	public static String getConnectionHTML() {
		StringBuilder sb = new StringBuilder(), conns = new StringBuilder();

		Node node;

		DBCursor cursor = Config.get_nodes().find();

		while (cursor.hasNext()) {
			node = new Node(cursor.next());

			for (DBObject conn : node.getConnections().toArray(new DBObject[0])) {
				if (!conns.toString().isEmpty())
					conns.append(",");

				conns.append(conn.toString());
			}
		}
		sb.append("<div id=\"connections\" class=\"connections\" style=\"display: none;\">{\"connections\":[");
		sb.append(conns.toString());
		sb.append("]}</div>");

		return sb.toString();
	}

	public static String getGraphHTML() {
		StringBuilder sb = new StringBuilder();

		Node node;

		DBCursor cursor = Config.get_nodes().find();

		while (cursor.hasNext()) {
			node = new Node(cursor.next());
			sb.append("<div class=\"w\" id=\"");
			sb.append(node.getName());
			sb.append("\" style=\"top:");
			sb.append(node.getY());
			sb.append("em; left:");
			sb.append(node.getX());
			sb.append("em;\">");
			sb.append(node.getName());
			sb.append("<br>");
			sb.append(node.getHost());
			sb.append("<br>");
			sb.append(node.getState());
			sb.append("</div>");
		}

		return sb.toString();
	}

	private BasicDBList getConnections() {
		return (BasicDBList) _doc.get("connections");
	}

	public void addConnection(String name, String label) {
		Config.get_nodes().update(
				new BasicDBObject("_id", _doc.getObjectId("_id")),
				new BasicDBObject("$addToSet", new BasicDBObject("connections", new BasicDBObject("source", getName())
						.append("target", name).append("label", label))));

		refresh();
	}

	public void removeConnection(String name, String label) {
		BasicDBObject obj = new BasicDBObject("$pull", new BasicDBObject("connections", new BasicDBObject("source",
				getName()).append("target", name).append("label", label)));

		Config.get_nodes().update(new BasicDBObject("_id", _doc.getObjectId("_id")), obj);

		refresh();
	}
}
