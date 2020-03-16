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
import com.google.common.base.Strings;
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

	public LevelDBDataLayer(final File parent) {
		this.parent = parent;
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
		} catch (IOException ex) {
			LOGGER.error("", ex);
			throw new RuntimeException(ex);
		}
	}

	@Override
	public <T extends Data> Optional<T> get(final String uid, final String key, final Class<T> clazz) {
		final String data = asString(db.get(bytes(uuid(uid, key))));
		if (Strings.isNullOrEmpty(data)){
			Optional.empty();
		}
		T value = gson.fromJson(data, clazz);
		return Optional.of(value);
	}

	@Override
	public void remove(final String uid, final String key) {
		db.delete(bytes(uuid(uid, key)));
	}

	@Override
	public boolean add(String uid, String key, Data value) {
		try (StringWriter writer = new StringWriter();) {
			gson.toJson(value, writer);
			final String uuid = uuid(uid, key);
			db.put(bytes(uuid), bytes(writer.toString()));
			return true;
		} catch (IOException ex) {
			LOGGER.error("", ex);
			throw new RuntimeException(ex);
		}
	}

	@Override
	public boolean exists(final String uid, final String key) {
		byte[] content = db.get(bytes(uuid(uid, key)));
		return content != null && content.length > 0;
	}

	public static String uuid(final String uid, final String key) {
		return String.format("%s/%s", uid, key);
	}

	public static String[] split(final String key) {
		return key.split("/");
	}
}
