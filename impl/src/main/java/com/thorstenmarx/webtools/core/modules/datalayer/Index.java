/*
 * Copyright (C) 2019 Thorsten Marx
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.thorstenmarx.webtools.core.modules.datalayer;

/*-
 * #%L
 * webtools-cluster
 * %%
 * Copyright (C) 2016 - 2019 Thorsten Marx
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-3.0.html>.
 * #L%
 */
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author marx
 */
public class Index {
	
	private final MultiMap<String, String> byUserKey;
	private final MultiMap<String, String> byKey;

	protected Index() {
		byUserKey = new MultiMap<>();
		byKey = new MultiMap<>();
	}

	public void clear() {
		byUserKey.clear();
		byKey.clear();
	}

	public void addByUserKey(final String uid, final String key, final String uuid) {
		byUserKey.put(user_key(uid, key), uuid);
		byKey.put(key, uuid);
	}

	public boolean containsByUserKey(final String uid, final String key) {
		return byUserKey.containsKey(user_key(uid, key));
	}

	public Set<String> findByUserKey(final String uid, final String key) {
		return byUserKey.get(user_key(uid, key));
	}
	public Set<String> findByKey(final String key) {
		return new HashSet<>(byKey.get(key));
	}

	public void removeByUserKey(final String uid, final String key) {
		Set<String> uuids = byUserKey.get(user_key(uid, key));
		uuids.forEach((uuid) ->{
			byKey.remove(key, uuid);
		});
		byUserKey.remove(user_key(uid, key));
	}

	public String user_key(final String uid, final String key) {
		return String.format("%s/%s", uid, key);
	}
	
}
