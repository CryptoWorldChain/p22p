package org.fc.brewchain.p22p;

import org.brewchain.bcapi.backend.ODBDao;

import onight.tfw.ojpa.api.ServiceSpec;

public class ODSViewStateStorage extends ODBDao {

	public ODSViewStateStorage(ServiceSpec serviceSpec) {
		super(serviceSpec);
	}

	@Override
	public String getDomainName() {
		return "pzp_viewstate";
	}

	
}
