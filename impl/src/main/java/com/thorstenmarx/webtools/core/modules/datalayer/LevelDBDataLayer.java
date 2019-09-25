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
 * webtools-datalayer
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
import com.google.gson.Gson;
import com.thorstenmarx.webtools.api.datalayer.Data;
import com.thorstenmarx.webtools.api.datalayer.DataLayer;
import java.io.File;
import org.slf4j.Logger;
import org.iq80.leveldb.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.iq80.leveldb.impl.Iq80DBFactory;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.slf4j.LoggerFactory;

/**
 *
 * @author marx
 */
public class LevelDBDataLayer implements DataLayer {

	private static final Logger LOGGER = LoggerFactory.getLogger(LevelDBDataLayer.class);

	final File parent;

	private Gson gson = new Gson();
	private DB db;

	private final Index index;

	public LevelDBDataLayer(final File parent) {
		this.parent = parent;

		index = new Index();
	}

	public void close() {
		try {
			db.close();
		} catch (IOException ex) {
			LOGGER.error("", ex);
			throw new RuntimeException(ex);
		}
	}

	public void open() {
		try {
			File folder = new File(parent, "datalayer/");
			if (!folder.exists()) {
				folder.mkdirs();
			}
			Options options = new Options();
			options.createIfMissing(true);
			DBFactory factory = new Iq80DBFactory();

			db = factory.open(new File(folder, "leveldb"), options);

			// build index
			try (DBIterator iterator = db.iterator()) {
				for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
					final String stringKey = asString(iterator.peekNext().getKey());
					final String[] splitted = split(stringKey);

					index.addByUserKey(splitted[0], splitted[1], stringKey);
				}
			}
		} catch (IOException ex) {
			LOGGER.error("", ex);
			throw new RuntimeException(ex);
		}
	}

	@Override
	public <T extends Data> Optional<T> get(String uid, String key, Class<T> clazz) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void remove(final String uid, final String key) {
		db.delete(bytes(uuid(uid, key)));
		index.removeByUserKey(uid, key);
	}

	@Override
	public boolean add(String uid, String key, Data value) {
		try (StringWriter writer = new StringWriter();) {
			gson.toJson(value, writer);
			return internal_add(uid, key, writer.toString());
		} catch (IOException ex) {
			LOGGER.error("", ex);
			throw new RuntimeException(ex);
		}
	}
	
	protected boolean internal_add (final String uid, final String key, final String value) {
		final String uuid = uuid(uid, key);
		db.put(bytes(uuid), bytes(value));
		index.addByUserKey(uid, key, uuid);
		
		return true;
	}
	
	
	@Override
	public boolean exists(String uid, String key) {
		return index.containsByUserKey(uid, key);
	}

	public static String uuid(final String uid, final String key) {
		return String.format("%s/%s/%s", uid, key, UUID.randomUUID().toString());
	}

	public static String[] split(final String key) {
		return key.split("/");
	}

	@Override
	public <T extends Data> void each(final BiConsumer<String, T> consumer, final String key, final Class<T> clazz) {
		Set<String> uuids = index.findByKey(key);
		if (uuids.isEmpty()) {
			return;
		}
		uuids.forEach((uuid) -> {
			final String uid = split(uuid)[0];
			final String data = asString(db.get(bytes(uuid)));
			consumer.accept(uid, gson.fromJson(data, clazz));
		});
	}

	@Override
	public <T extends Data> Optional<List<T>> list(String uid, String key, Class<T> clazz) {
		if (!index.containsByUserKey(uid, key)) {
			return Optional.empty();
		}
		List<T> result = new ArrayList<>();
		Set<String> uuids = index.findByUserKey(uid, key);
		uuids.forEach((uuid) -> {
			final String data = asString(db.get(bytes(uuid)));
			result.add(gson.fromJson(data, clazz));
		});

		return Optional.of(result);
	}

	@Override
	public void clear(String key) {
		final Set<String> uuids = index.findByKey(key);
		uuids.forEach((uuid) -> {
			final String [] splitted = split(uuid);
			remove(splitted[0], splitted[1]);
		});
	}
}
