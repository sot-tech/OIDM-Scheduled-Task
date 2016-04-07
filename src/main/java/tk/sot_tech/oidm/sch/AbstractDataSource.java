/*
 * Copyright (c) 2016, eramde
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package tk.sot_tech.oidm.sch;

import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Logger;
import oracle.iam.platform.entitymgr.provider.ldap.CaseInsensitiveKeysMap;
import tk.sot_tech.oidm.utility.ITResourceUtility;
import tk.sot_tech.oidm.utility.Misc;
import static tk.sot_tech.oidm.utility.Misc.isNullOrEmpty;

public abstract class AbstractDataSource implements AutoCloseable {
	
	public static interface Stop {
		void isTerminated() throws SheduledTaskTerminatedException;
	}

	public static final String MULTIVALUED_KEY = "{!MULTIVALUE!}",
			REVOKED_KEY = "{!REVOKED!}",
			INCLUDE_IT_PARAMETER = "includeit";

	protected ReconParameters parameters;
	protected Stop task = new Stop() {
		@Override
		public void isTerminated() throws SheduledTaskTerminatedException {
			
		}
	};
	protected boolean includeItResource;
	protected long itKey = 0;

	public AbstractDataSource init(ReconParameters reconParameters) {
		this.parameters = reconParameters;
		CaseInsensitiveKeysMap<String, String> params = reconParameters.getParameters();
		if (params != null) {
			String inclIt = params.get(INCLUDE_IT_PARAMETER);

			if (includeItResource = (!isNullOrEmpty(inclIt) && "true".equalsIgnoreCase(inclIt))) {
				try(ITResourceUtility itUtil = new ITResourceUtility()){
					itKey = itUtil.getITResourceKey(reconParameters.getItResourceName());
				}
				catch (tcAPIException | tcColumnNotFoundException ex) {
					Logger.getLogger(AbstractDataSource.class.getName()).severe(Misc.ownStack(ex));
					includeItResource = false;
				}
			}
		}
		return initImpl();
	}
	
	public AbstractDataSource setStopDelegate(Stop stop){
		task = stop;
		return this;
	}

	protected void checkAndAppendITResourceKey(HashMap<String, Object> row) {
		if (includeItResource) {
			for (String field : row.keySet()) {
				Object value = row.get(field);
				if (value instanceof String) {
					String tmp = String.format("%d~%s", itKey, (String) value);
					row.put(field, tmp);
				}
			}
		}
	}

	protected abstract AbstractDataSource initImpl();

	public abstract ArrayList<HashMap<String, Object>> fetchData() throws Exception, SheduledTaskTerminatedException;

	public abstract void clearData(ArrayList<HashMap<String, Object>> values) throws Exception, SheduledTaskTerminatedException;

}
