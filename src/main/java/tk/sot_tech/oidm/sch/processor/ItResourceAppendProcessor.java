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
import java.util.HashMap;
import java.util.logging.Logger;
import tk.sot_tech.oidm.sch.AbstractProcessor;
import tk.sot_tech.oidm.utility.ITResourceUtility;
import static tk.sot_tech.oidm.utility.Misc.isNullOrEmpty;
import static tk.sot_tech.oidm.utility.Misc.ownStack;

public class ItResourceAppendProcessor extends AbstractProcessor {
	
	public static final String IT_RECOURCE_FIELD = "IT_RESOURCE";
	private long itResourceKey = -1;

	@Override
	public boolean preprocess(HashMap<String, Object> in) {
		if(!isNullOrEmpty(in) && itResourceKey >= 0){
			in.put(IT_RECOURCE_FIELD, itResourceKey);
			return true;
		}
		return false;
	}

	@Override
	protected void initImpl() {
		try(ITResourceUtility itru = new ITResourceUtility()) {
			itResourceKey = itru.getITResourceKey(parameters.getItResourceName());
		}
		catch (tcAPIException | tcColumnNotFoundException ex) {
			Logger.getLogger(ItResourceAppendProcessor.class.getName()).severe(ownStack(ex));
		}
	}
	
}
