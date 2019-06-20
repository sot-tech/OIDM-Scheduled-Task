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
import oracle.iam.scheduler.api.SchedulerService;
import oracle.iam.scheduler.vo.JobDetails;
import oracle.iam.scheduler.vo.JobParameter;
import oracle.iam.scheduler.vo.TaskSupport;
import tk.sot_tech.oidm.utility.Misc;

public abstract class AbstractScheduledTask extends TaskSupport {
	
	private static final Logger LOG = Logger.getLogger(AbstractScheduledTask.class.getName());

	protected void isTerminated() throws SheduledTaskTerminatedException {
		if (isStop()) {
			throw new SheduledTaskTerminatedException();
		}
	}

	@Override
	public void execute(HashMap params) throws Exception {
		try {
			engage(params);
		} catch (SheduledTaskTerminatedException ignore) {
			Logger.getLogger(AbstractScheduledTask.class.getName()).log(Level.WARNING, "Task {0} has stopped", getName());
		}
	}

	protected abstract void engage(HashMap params) throws SheduledTaskTerminatedException, Exception;

	@Override
	public HashMap getAttributes() {
		return null;
	}

	@Override
	public void setAttributes() {
	}

	public static void updateReconField(String fieldName, String value, String taskName) throws tcAPIException, Exception {
		boolean success = false;
		tcSchedulerOperationsIntf scheduleIntf = null;
		LOG.warning("Try to set with old API");
		try {
			scheduleIntf = tk.sot_tech.oidm.utility.Platform.getService(tcSchedulerOperationsIntf.class);
			HashMap<String, String> tmp = new HashMap<>();
			tmp.put("Task Scheduler.Name", taskName);
			tcResultSet found = scheduleIntf.findScheduleTasks(tmp);
			if (!Misc.isNullOrEmpty(found)) {
				found.goToRow(0);
				long schedKey = found.getLongValue("Task Scheduler.Key");
				if (schedKey > 0) {
					tmp.clear();
					tmp.put("Task Scheduler.Key", String.valueOf(schedKey));
					tmp.put("Task Scheduler.Task Attributes.Name", fieldName);
					found = scheduleIntf.findScheduleTaskAttributes(tmp);
					if (!Misc.isNullOrEmpty(found)) {
						found.goToRow(0);
						long fieldKey = found.getLongValue("Task Scheduler.Task Attributes.Key");
						if (fieldKey > 0) {
							tmp.put("Task Scheduler.Task Attributes.Value", value);
							scheduleIntf.updateScheduleTaskAttribute(schedKey, fieldKey, tmp);
							success = true;
						}
					}
				}
			}
		} catch (Exception ex) {
			LOG.log(Level.WARNING, "Unable to update attrubute with old API, {0} Trying with new API", ex.toString());
		} finally {
			if (scheduleIntf != null) {
				scheduleIntf.close();
			}
		}
		if (!success) {
			SchedulerService newService = tk.sot_tech.oidm.utility.Platform.getService(SchedulerService.class);
			JobDetails jobDetail = newService.getJobDetail(taskName);
			if (jobDetail != null) {
				HashMap<String, JobParameter> params = jobDetail.getParams();
				if (!Misc.isNullOrEmpty(params)) {
					JobParameter param = params.get(fieldName);
					if (param == null) {
						param = new JobParameter();
						param.setName(fieldName);
						param.setDataType("string");
						param.setValue(value);
						params.put(fieldName, param);
					} else {
						param.setValue(value);
					}
					newService.updateJob(jobDetail);
					success = true;
				}
			}
		}
		if (!success) {
			LOG.log(Level.WARNING, "Unable to set value [{0}] to field [{1}] in task \"{2}\"",
																		new Object[]{value, fieldName, taskName});
		}
	}

	protected void updateReconField(String fieldName, String value) throws Exception {
		updateReconField(fieldName, value, getName());
	}

}
