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
import Thor.API.Operations.tcSchedulerOperationsIntf;
import Thor.API.tcResultSet;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import oracle.iam.scheduler.vo.TaskSupport;
import tk.sot_tech.oidm.utility.Platform;

public abstract class AbstractScheduledTask extends TaskSupport {
	
	protected void isTerminated() throws SheduledTaskTerminatedException {
		if (isStop()) {
			throw new SheduledTaskTerminatedException();
		}
	}
	
	@Override
	public void execute(HashMap params) throws Exception{
		try{
			engage(params);
		}
		catch(SheduledTaskTerminatedException ignore){
			Logger.getLogger(AbstractScheduledTask.class.getName()).log(Level.WARNING, "Task {0} has stopped", getName());
		}
	}
	
	protected abstract void engage(HashMap params) throws SheduledTaskTerminatedException, Exception;
	
	@Override
	public HashMap getAttributes() {
		return null;
	}

	@Override
	public void setAttributes() {}
	
	public static void updateReconField(String fieldName, String value, String taskName) throws tcAPIException, Exception {
		String itsName = taskName;
		long schedKey, fieldKey;
		tcSchedulerOperationsIntf scheduleIntf = Platform.getService(tcSchedulerOperationsIntf.class);
		HashMap<String, String> tmp = new HashMap<>();
		tmp.put("Task Scheduler.Name", itsName);
		tcResultSet found = scheduleIntf.findScheduleTasks(tmp);
		found.goToRow(0);
		schedKey = found.getLongValue("Task Scheduler.Key");
		tmp.clear();
		tmp.put("Task Scheduler.Key", String.valueOf(schedKey));
                tmp.put("Task Scheduler.Task Attributes.Name", fieldName);
		found = scheduleIntf.findScheduleTaskAttributes(tmp);
		found.goToRow(0);
		fieldKey = found.getLongValue("Task Scheduler.Task Attributes.Key");
		tmp.put("Task Scheduler.Task Attributes.Value", value);

		scheduleIntf.updateScheduleTaskAttribute(schedKey, fieldKey, tmp);

	}
	
	protected void updateReconField(String fieldName, String value) throws Exception{
		updateReconField(fieldName, value, getName());
	}
	
}
