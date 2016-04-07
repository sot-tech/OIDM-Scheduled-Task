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
import Thor.API.Exceptions.tcITResourceNotFoundException;
import java.net.MalformedURLException;
import java.util.Date;
import java.util.HashMap;
import java.util.ResourceBundle;
import oracle.iam.platform.entitymgr.provider.ldap.CaseInsensitiveKeysMap;
import tk.sot_tech.oidm.utility.ITResourceUtility;
import static tk.sot_tech.oidm.utility.Misc.getBundleFromFs;
import static tk.sot_tech.oidm.utility.Misc.isNullOrEmpty;

public class ReconParameters {

	public static final String OIM_OBJECT_STATUS = "OIM_OBJECT_STATUS";

	private String itResourceName;
	private HashMap<String, String> itParameters;
	private ResourceBundle resourceBundle;
	private final CaseInsensitiveKeysMap<String, String> parameters = new CaseInsensitiveKeysMap<>();
	private Date fromDate;

	public final void init(String itResourceName, String propertyFile,
						   String params, Date timeStamp) throws tcAPIException,
																 tcITResourceNotFoundException,
																 tcColumnNotFoundException,
																 MalformedURLException {
		this.itResourceName = itResourceName;
		if (!isNullOrEmpty(propertyFile) && !"-".equals(propertyFile) && !"NONE".equalsIgnoreCase(propertyFile)) {
			resourceBundle = getBundleFromFs(propertyFile);
		}
		if(isNullOrEmpty(itResourceName) || itResourceName.equals("-") || itResourceName.equalsIgnoreCase("NONE")){
			itParameters = new HashMap<>();
		}
		else{
			try(ITResourceUtility itru = new ITResourceUtility()){
				itParameters = itru.getITResourceParameters(itResourceName);
			}
		}
		this.fromDate = timeStamp;
		if (!isNullOrEmpty(params)) {
			for (String s : params.split(",")) {
				if (isNullOrEmpty(s)) {
					continue;
				}
				if (s.contains("=")) {
					String[] param = s.split("=");
					if (!isNullOrEmpty(param[0]) && !isNullOrEmpty(param[1])) {
						parameters.put(param[0].trim(), param[1].trim());
					}
				}
			}
		}
	}

	public CaseInsensitiveKeysMap<String, String> getParameters() {
		return parameters;
	}

	public void appendParameters(String key, String value) {
		parameters.put(key, value);
	}

	public String getItResourceName() {
		return itResourceName;
	}

	public HashMap<String, String> getItParameters() {
		return itParameters;
	}

	public ResourceBundle getResourceBundle() {
		return resourceBundle;
	}

	public Date getFromDate() {
		return fromDate;
	}

	@Override
	public String toString() {
		String resources = null;
		if (resourceBundle != null) {
			StringBuilder sb = new StringBuilder();
			for(String key : resourceBundle.keySet()){
				sb.append(key).append("=").append(resourceBundle.getString(key)).append('\n');
			}
			resources = sb.toString();
		}
		return "ReconParameters{" + "itResourceName=" + itResourceName
			   + ", resourceBundle=" + resources
			   + ", parameters=" + parameters
			   + ", fromDate=" + fromDate + '}';
	}

}
