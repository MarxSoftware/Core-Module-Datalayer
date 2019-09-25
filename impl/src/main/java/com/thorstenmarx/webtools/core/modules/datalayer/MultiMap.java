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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author marx
 */
public class MultiMap<K, V> {
	
	private final Map<K, Set<V>> multimap = new HashMap<>();

	public void put(K key, V value) {
		this.multimap.computeIfAbsent(key, (k) -> new HashSet<>()).add(value);
	}

	public Set<V> get(K key) {
		return this.multimap.getOrDefault(key, Collections.emptySet());
	}

	public void remove(K key, V value) {
		this.multimap.computeIfPresent(key, (k, set) -> set.remove(value) && set.isEmpty() ? null : set);
	}

	public void remove(K key) {
		this.multimap.remove(key);
	}

	public boolean contains(K key, V value) {
		return this.multimap.getOrDefault(key, Collections.emptySet()).contains(value);
	}

	public boolean containsKey(K key) {
		return this.multimap.containsKey(key);
	}

	public void clear() {
		this.multimap.clear();
	}

	public int size() {
		return this.multimap.values().stream().mapToInt(Set::size).sum();
	}

	public Set<V> values() {
		return (Set<V>) multimap.values().stream().flatMap(Set::stream);
	}
	
}
