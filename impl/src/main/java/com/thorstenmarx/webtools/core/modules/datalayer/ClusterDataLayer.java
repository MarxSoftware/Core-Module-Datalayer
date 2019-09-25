/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thorstenmarx.webtools.core.modules.datalayer;

import com.google.gson.Gson;
import com.thorstenmarx.webtools.api.cluster.Cluster;
import com.thorstenmarx.webtools.api.cluster.Message;
import com.thorstenmarx.webtools.api.cluster.services.MessageService;
import com.thorstenmarx.webtools.api.datalayer.Data;
import com.thorstenmarx.webtools.api.datalayer.DataLayer;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author marx
 */
public class ClusterDataLayer implements DataLayer, MessageService.MessageListener{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ClusterDataLayer.class);

	public static final String DATALAYER_ADD = "datalayer_add";
	public static final String DATALAYER_REMOVE = "datalayer_remove";
	public static final String DATALAYER_CLEAR = "datalayer_clear";
	
	private final LevelDBDataLayer wrapped_layer;
	private final Cluster cluster;
	
	private final Gson gson = new Gson();

	public ClusterDataLayer(final LevelDBDataLayer wrapped_layer, final Cluster cluster) {
		this.wrapped_layer = wrapped_layer;
		this.cluster = cluster;
		
		cluster.getMessageService().registerMessageListener(this);
	}
	
	public void close () {
		cluster.getMessageService().unregisterMessageListener(this);
	}
	
	@Override
	public <T extends Data> Optional<T> get(String uid, String key, Class<T> clazz) {
		return this.wrapped_layer.get(uid, key, clazz);
	}

	@Override
	public <T extends Data> Optional<List<T>> list(String uid, String key, Class<T> clazz) {
		return this.wrapped_layer.list(uid, key, clazz);
	}

	@Override
	public boolean add(String uid, String key, Data value) {
		PayloadAdd payload = new PayloadAdd();
		payload.uid = uid;
		payload.key = key;
		payload.value = gson.toJson(value);
		
		Message message = new Message();
		message.setType(DATALAYER_ADD);
		message.setPayload(gson.toJson(payload));
		try {	
			cluster.getMessageService().publish(message);
			return true;
		} catch (IOException ex) {
			LOGGER.error("", ex);
		}
		return false;
	}

	@Override
	public boolean exists(String uid, String key) {
		return this.wrapped_layer.exists(uid, key);
	}

	@Override
	public void remove(String uid, String key) {
		PayloadRemove payload = new PayloadRemove();
		payload.uid = uid;
		payload.key = key;
		
		Message message = new Message();
		message.setType(DATALAYER_REMOVE);
		message.setPayload(gson.toJson(payload));
		try {	
			cluster.getMessageService().publish(message);
		} catch (IOException ex) {
			LOGGER.error("", ex);
		}
	}

	@Override
	public void clear(String key) {
		PayloadClear payload = new PayloadClear();
		payload.key = key;
		
		Message message = new Message();
		message.setType(DATALAYER_CLEAR);
		message.setPayload(gson.toJson(payload));
		try {	
			cluster.getMessageService().publish(message);
		} catch (IOException ex) {
			LOGGER.error("", ex);
		}
	}

	@Override
	public <T extends Data> void each(BiConsumer<String, T> consumer, String key, Class<T> clazz) {
		wrapped_layer.each(consumer, key, clazz);
	}

	@Override
	public void handle(Message message) {
		if (DATALAYER_ADD.equals(message.getType())) {
			PayloadAdd payload = gson.fromJson(message.getPayload(), PayloadAdd.class);
			wrapped_layer.internal_add(payload.uid, payload.key, payload.value);
		} else if (DATALAYER_CLEAR.equals(message.getType())) {
			PayloadClear payload = gson.fromJson(message.getPayload(), PayloadClear.class);
			wrapped_layer.clear(payload.key);
		} else if (DATALAYER_REMOVE.equals(message.getType())) {
			PayloadRemove payload = gson.fromJson(message.getPayload(), PayloadRemove.class);
			wrapped_layer.remove(payload.uid, payload.key);
		}
	}

	public static class PayloadAdd {
		private String uid;
		private String key;
		private String value;
	}
	public static class PayloadRemove {
		private String uid;
		private String key;
	}
	public static class PayloadClear {
		private String key;
	}
	
}
