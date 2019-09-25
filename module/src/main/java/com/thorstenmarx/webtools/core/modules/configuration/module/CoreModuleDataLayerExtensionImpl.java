package com.thorstenmarx.webtools.core.modules.configuration.module;

import com.thorstenmarx.modules.api.annotation.Extension;
import com.thorstenmarx.webtools.api.datalayer.DataLayer;
import com.thorstenmarx.webtools.api.extensions.core.CoreDataLayerExtension;

/**
 *
 * @author marx
 */
@Extension(CoreDataLayerExtension.class)
public class CoreModuleDataLayerExtensionImpl extends CoreDataLayerExtension {

	@Override
	public String getName() {
		return "CoreModule Configuration";
	}

	@Override
	public DataLayer getDataLayer() {
		return CoreModuleDatalayerModuleLifeCycle.dataLayer;
	}

	@Override
	public void init() {
	}
	
}
