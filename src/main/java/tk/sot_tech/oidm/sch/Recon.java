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
import java.io.Serializable;
import java.net.MalformedURLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import static oracle.iam.reconciliation.api.ChangeType.DELETE;
import static oracle.iam.reconciliation.api.ChangeType.REGULAR;
import oracle.iam.reconciliation.api.*;
import static oracle.iam.reconciliation.vo.EventConstants.RECON_EVENT_KEY;
import oracle.iam.reconciliation.vo.ReconEvent;
import oracle.iam.reconciliation.vo.ReconSearchCriteria;
import static oracle.iam.reconciliation.vo.ReconSearchCriteria.Operator.EQUAL;
import static tk.sot_tech.oidm.sch.AbstractDataSource.REVOKED_KEY;
import tk.sot_tech.oidm.utility.Misc;
import static tk.sot_tech.oidm.utility.Misc.isNullOrEmpty;
import tk.sot_tech.oidm.utility.Platform;

public class Recon extends AbstractScheduledTask {

	private static final ResourceBundle BUNDLE = ResourceBundle.getBundle("resources/recon");
	
	private static final Logger LOG = Logger.getLogger(Recon.class.getName());

	public static final String PARAM_PREPROCESSOR = BUNDLE.getString("scheduler.preprocessor"),
		PARAM_IT_RESOURCE_NAME = BUNDLE.getString("scheduler.itresource"),
		PARAM_RESOURCE_OBJECT = BUNDLE.getString("scheduler.object"),
		PARAM_ADDITIONAL = BUNDLE.getString("scheduler.additional"),
		PARAM_PROPERTY_FILE = BUNDLE.getString("scheduler.sqlparampath"),
		PARAM_TIMESTAMP = BUNDLE.getString("scheduler.timestamp"),
		PARAM_DATASOURCE = BUNDLE.getString("scheduler.datasource"),
		DATE_FORMAT = BUNDLE.getString("param.dateformat"),
		DONT_PROCESS_KEY = "noprocess",
		DONT_CLEAR_KEY = "noclear",
		BATCH_SIZE_KEY = "batch",
		LIMIT_SIZE_KEY = "limit",
		IGNORE_EVENT_STATUS = "Creation Succeeded";

	private final SimpleDateFormat ORACLE_SQL_TIMESTAMP_FORMAT = new SimpleDateFormat(DATE_FORMAT);

	private AbstractProcessor preProcessor = null;
	private ReconParameters parameters;
	private String resourceObject;

	private int PARTIAL_BATCH_SIZE = 50, LIMIT_SIZE = 0;

	@Override
	public void engage(HashMap hm) throws Exception {

		ArrayList<HashMap<String, Object>> values;
		final Recon r = this;
		try (AbstractDataSource dataSource = init(hm)) {
			values = dataSource.setStopDelegate(new AbstractDataSource.Stop() {
				@Override
				public void isTerminated() throws SheduledTaskTerminatedException {
					r.isTerminated();
				}
			}).fetchData();
		}
		String tmp = parameters.getParameters().get(DONT_PROCESS_KEY);
		if (!Misc.toBoolean(tmp) && 
				!isNullOrEmpty(values)) {
			process(values);
		}
		tmp = parameters.getParameters().get(DONT_CLEAR_KEY);
		if (!Misc.toBoolean(tmp) && 
				!isNullOrEmpty(values)) {
			try (AbstractDataSource dataSource = ((AbstractDataSource) Class.forName(tmp).
												  newInstance()).init(parameters)) {
													  dataSource.setStopDelegate(
														  new AbstractDataSource.Stop() {
														  @Override
														  public void isTerminated() throws
															  SheduledTaskTerminatedException {
															  r.isTerminated();
														  }
													  }).clearData(values);
												  }
		}
		tmp = parameters.getParameters().get(BATCH_SIZE_KEY);
		if (!isNullOrEmpty(tmp)) {
			PARTIAL_BATCH_SIZE = Integer.decode(tmp);
		}
		tmp = parameters.getParameters().get(LIMIT_SIZE_KEY);
		if (!isNullOrEmpty(tmp)) {
			LIMIT_SIZE = Integer.decode(tmp);
		}
		updateReconField(PARAM_TIMESTAMP, ORACLE_SQL_TIMESTAMP_FORMAT.format(new Date()));
	}

	private AbstractDataSource init(HashMap params) throws ClassNotFoundException,
														   InstantiationException,
														   IllegalAccessException,
														   tcAPIException,
														   ParseException,
														   tcColumnNotFoundException,
														   tcITResourceNotFoundException,
														   SheduledTaskTerminatedException,
														   MalformedURLException {

		String tmp = ((String) params.get(PARAM_PREPROCESSOR)).trim();
		Date fromDate = new Date(0);
		if (!isNullOrEmpty(tmp)) {
			preProcessor = (AbstractProcessor) Class.forName(tmp).newInstance();
		}
		tmp = (String) params.get(PARAM_TIMESTAMP);
		if (!isNullOrEmpty(tmp)) {
			fromDate = ORACLE_SQL_TIMESTAMP_FORMAT.parse(tmp.trim());
		}
		resourceObject = (String) params.get(PARAM_RESOURCE_OBJECT);
		parameters = new ReconParameters();
		parameters.init(((String) params.get(PARAM_IT_RESOURCE_NAME)).trim(), (String) params.get(PARAM_PROPERTY_FILE), (String) params.
						get(PARAM_ADDITIONAL), fromDate);
		LOG.log(Level.FINE, "Recon parameters {0}", parameters.toString());
		tmp = ((String) params.get(PARAM_DATASOURCE)).trim();
		if (isNullOrEmpty(tmp)) {
			throw new IllegalArgumentException(PARAM_DATASOURCE + " is empty");
		}
		if (preProcessor != null) {
			preProcessor.init(parameters);
		}
		AbstractDataSource dataSource = (AbstractDataSource) Class.forName(tmp).newInstance();
		return dataSource.init(parameters);
	}

	private void process(ArrayList<HashMap<String, Object>> values) throws
		SheduledTaskTerminatedException {
		ArrayList<HashMap<String, Object>> reconData = new ArrayList<>();
		ArrayList<HashMap<String, Object>> toRemove = new ArrayList<>();
		int total = values.size(), current = 0;
		LOG.log(Level.FINE, "Preprocessing...");
		ReconOperationsService reconSvc = Platform.getService(ReconOperationsService.class);
		EventMgmtService reconMng = Platform.getService(EventMgmtService.class);
		if (LIMIT_SIZE > 0 && total >= LIMIT_SIZE) {
			throw new IllegalArgumentException(
				"Unable to process record count more than limit: " + total);
		} else {
			for (HashMap<String, Object> record : values) {
				boolean preprocessed = true;
				isTerminated();
				if (preProcessor != null) {
					LOG.log(Level.FINEST, "Processing record {0}", record);
					preprocessed = preProcessor.preprocess(record);
				}
				if (preprocessed) {
					reconData.add(record);
				}
				if (reconData.size() == PARTIAL_BATCH_SIZE) {
					LOG.log(Level.INFO,
																"Partial Reconcilating...");
					processPartial(reconData, reconSvc, reconMng);
					toRemove.addAll(reconData);
					reconData.clear();
				}
				if (current > 0 && (current * 100 / total) % 20 == 0) {
					LOG.log(Level.INFO,
																"{0} of {1} records preprocessed",
																new Object[]{current, total});
				}
			}
			LOG.log(Level.INFO, "Finish Reconcilating...");
			processPartial(reconData, reconSvc, reconMng);
			toRemove.addAll(reconData);
			values.clear();
			values.addAll(toRemove);
		}
	}

	private void processPartial(ArrayList<HashMap<String, Object>> reconData,
								ReconOperationsService reconSvc, EventMgmtService reconMng) throws
		SheduledTaskTerminatedException {
		if(Misc.isNullOrEmpty(resourceObject)){
			throw new IllegalArgumentException("Missing parameter " + PARAM_RESOURCE_OBJECT);
		}
		if (!isNullOrEmpty(reconData)) {
			BatchAttributes ba = new BatchAttributes(resourceObject, DATE_FORMAT, true);
			InputData[] input = new InputData[reconData.size()];
			int i = 0;
			for (HashMap<String, Object> record : reconData) {
				isTerminated();
				HashMap<String, Object> multival
										= (HashMap) record.
						remove(AbstractDataSource.MULTIVALUED_KEY);
				Object o = record.remove(REVOKED_KEY);
				boolean revoked = o != null && ((Boolean) o);
				input[i++] = new InputData((Map) record, (Map) multival, true,
										   revoked ? DELETE : REGULAR, null);
			}
			ReconciliationResult reconEvents = reconSvc.createReconciliationEvents(ba, input);
			for (Serializable key : reconEvents.getSuccessResult()) {
				isTerminated();
				try {
					ReconSearchCriteria reconSearchCriteria = new ReconSearchCriteria();
					reconSearchCriteria.addExpression(RECON_EVENT_KEY, key, EQUAL);
					List<ReconEvent> search = reconMng.search(reconSearchCriteria, new Vector(),
															  true, 0, 100);
					if (search != null && !search.isEmpty() && !IGNORE_EVENT_STATUS.
						equalsIgnoreCase(search.get(0).getReStatus())) {
						reconSvc.processReconciliationEvent((Long) key);
					}
				} catch (tcAPIException ex) {
					LOG.severe(Misc.ownStack(ex));
				}
			}
		}
	}

}
