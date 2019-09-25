/**
 * webTools-contentengine
 * Copyright (C) 2016  Thorsten Marx (kontakt@thorstenmarx.com)
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
package com.thorstenmarx.webtools.core.modules.configuration.module;

import com.thorstenmarx.modules.api.ModuleLifeCycleExtension;
import com.thorstenmarx.modules.api.annotation.Extension;
import com.thorstenmarx.webtools.api.CoreModuleContext;
import com.thorstenmarx.webtools.api.datalayer.DataLayer;
import com.thorstenmarx.webtools.core.modules.datalayer.ClusterDataLayer;
import com.thorstenmarx.webtools.core.modules.datalayer.LevelDBDataLayer;

/**
 *
 * @author marx
 */
@Extension(ModuleLifeCycleExtension.class)
public class CoreModuleDatalayerModuleLifeCycle extends ModuleLifeCycleExtension {

	public static DataLayer dataLayer;
	public static LevelDBDataLayer internal_dataLayer;
	public static ClusterDataLayer cluster_dataLayer;

	private CoreModuleContext getCoreContext () {
		return (CoreModuleContext)getContext();
	}
	
	@Override
	public void activate() {
		internal_dataLayer = new LevelDBDataLayer(configuration.getDataDir());
		internal_dataLayer.open();

		if (getCoreContext().isCluster()) {
			cluster_dataLayer = new ClusterDataLayer(internal_dataLayer, getCoreContext().getCluster());
			dataLayer = cluster_dataLayer;
		} else {
			dataLayer = internal_dataLayer;
		}
		
		getContext().serviceRegistry().register(DataLayer.class, dataLayer);
	}

	@Override
	public void deactivate() {
		getContext().serviceRegistry().unregister(DataLayer.class, dataLayer);
		if (getCoreContext().isCluster()) {
			cluster_dataLayer.close();
		}
		internal_dataLayer.close();
	}

	@Override
	public void init() {

	}

}
