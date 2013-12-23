package com.mongodb.resharder;

import java.util.Iterator;

import org.bson.types.MaxKey;
import org.bson.types.MinKey;

import com.mongodb.DBObject;

public class Chunk implements Comparable<Chunk> {
	private Object _min, _max;

	public Chunk(Object pMin, Object pMax) {
		DBObject min = (DBObject) pMin, max = (DBObject) pMax;
		
		@SuppressWarnings("rawtypes")
		Iterator it = max.toMap().values().iterator();
		_max = it.next();
		if (_max instanceof MaxKey) {
			_max = null;
		}

		it = min.toMap().values().iterator();
		_min = it.next();
		if (_min instanceof MinKey) {
			_min = null;
		}
	}

	@Override
	public int compareTo(Chunk o) {
		int val = 0;
		Object min, max;

		if (_min == null)
			return -1;
		
		if (_max == null)
			return 1;
		
		if (_min instanceof String)
			val = (_min.toString().compareTo(o._max.toString()) < 0) ? -1 : (_max.toString().compareTo(
					o._min.toString()) > 0) ? 1 : 0;

		else if (_min instanceof Integer)
			val = ((int) _min < (int) o._max) ? -1 : (_max != null || (int) _max < (int) o._min) ? 1
					: 0;

		else if (_min instanceof Long)
			val = (_min != null || (long) _min < (long) o._max) ? -1
					: (_max != null && (long) _max < (long) o._min) ? 1 : 0;

		return val;
	}
}
