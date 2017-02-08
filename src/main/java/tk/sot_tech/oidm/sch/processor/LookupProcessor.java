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
package tk.sot_tech.oidm.sch.processor;

import Thor.API.Exceptions.tcAPIException;
import Thor.API.Exceptions.tcColumnNotFoundException;
import Thor.API.Exceptions.tcInvalidLookupException;
import Thor.API.Exceptions.tcInvalidValueException;
import Thor.API.Operations.tcLookupOperationsIntf;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import tk.sot_tech.oidm.sch.AbstractProcessor;
import static tk.sot_tech.oidm.sch.Recon.DONT_CLEAR_KEY;
import tk.sot_tech.oidm.utility.ITResourceUtility;
import tk.sot_tech.oidm.utility.LookupUtility;
import tk.sot_tech.oidm.utility.Misc;
import static tk.sot_tech.oidm.utility.Misc.isNullOrEmpty;
import static tk.sot_tech.oidm.utility.Misc.ownStack;
import static tk.sot_tech.oidm.utility.Misc.trimStrings;

public class LookupProcessor extends AbstractProcessor {

	public static final String LOOKUP_KEY = "lookup",
		ID_FIELD = "ID", NAME_FIELD = "NAME",
		INCLUDE_IT_KEY = "includeit";
	private String lookupName = null;
	private HashMap<String, String> cache = null;
	private long itKey = 0;
	private boolean includeIt = false, clear = true;

	private String nullToNULL(String s) {
		if (s == null) {
			Logger.getLogger(LookupProcessor.class.getName()).log(Level.WARNING,
																  "Code/Decode is null, falling back to 'null' string");
			return "null";
		}
		return s;
	}

	@Override
	public boolean preprocess(HashMap<String, Object> in) {
		try (LookupUtility lookupUtil = new LookupUtility()) {
			if (clear) {
				final int maxDeleteTries = 5;
				int i = 0;
				try {
					do {
						cache = lookupUtil.clearLookup(lookupName);
						TimeUnit.SECONDS.sleep(1);
					} while (!cache.isEmpty() && i++ < maxDeleteTries);
					if (!cache.isEmpty()) {
						Logger.getLogger(tcLookupOperationsIntf.class.getName()).log(Level.WARNING,
																					 "Unable to delete lookups for using simple way");
					}
				} catch (tcAPIException | tcColumnNotFoundException | tcInvalidLookupException | InterruptedException ex) {
					Logger.getLogger(LookupProcessor.class.getName()).severe(ownStack(ex));
				}
			}
			
			trimStrings(in);

			try {
				if (cache == null) {
					cache = lookupUtil.getLookup(lookupName);
				}
				String code = nullToNULL(((String) in.get(ID_FIELD))).trim(), decode = nullToNULL(
					(String) in.get(NAME_FIELD)).trim();
				if (includeIt) {
					code = String.format("%d~%s", itKey, code);
					decode = parameters.getItResourceName() + '~' + decode;
				}

				boolean exist = false;
				if (cache.containsKey(code)) {
					String got = cache.get(code);
					if (!got.equalsIgnoreCase(decode)) {
						lookupUtil.getService().removeLookupValue(lookupName, code);
					} else {
						exist = true;
					}
				}
				if (!exist) {
					lookupUtil.getService().addLookupValue(lookupName, code, decode, "", "");
				}

			} catch (tcAPIException | tcInvalidLookupException | tcColumnNotFoundException | tcInvalidValueException ex) {
				Logger.getLogger(LookupProcessor.class.getName()).severe(ownStack(ex));
			}
		}

		return false;
	}

	@Override
	protected void initImpl() {
		String inclIt = parameters.getParameters().get(INCLUDE_IT_KEY);
		clear = !Misc.toBoolean(parameters.getParameters().get(DONT_CLEAR_KEY));
		includeIt = Misc.toBoolean(inclIt);
		if (includeIt) {
			try (ITResourceUtility itResUtil = new ITResourceUtility()) {
				itKey = itResUtil.getITResourceKey(parameters.getItResourceName());
			} catch (tcAPIException | tcColumnNotFoundException ex) {
				throw new IllegalStateException(ex);
			}
		}
		lookupName = parameters.getParameters().get(LOOKUP_KEY);
		if (isNullOrEmpty(lookupName)) {
			throw new NullPointerException(LOOKUP_KEY + " is null!");
		}
	}

}
