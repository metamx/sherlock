/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.yahoo.egads.control.ModelAdapter;
import com.yahoo.egads.control.ProcessableObject;
import com.yahoo.egads.control.ProcessableObjectFactory;
import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.model.EgadsResult;
import com.yahoo.sherlock.query.EgadsConfig;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.utils.EgadsUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * Service class for Egads API.
 */
@Slf4j
@Data
public class EgadsService {

    /**
     * To store the egads config.
     */
    private EgadsConfig p = null;

    /**
     * Default configuration of egads.
     */
    private void init() {
        p = EgadsConfig.create().buildDefault();
    }

    /**
     * Method to update base window of Olympic model based on period of timeseries.
     *
     * @param granularity granularity of data
     * @param granularityRange granularity to aggregate on
     */
    private void updateBaseWindow(Granularity granularity, Integer granularityRange) {
        String w1 = "1", w2 = "1";
        switch (granularity) {
            case MINUTE:
                w2 = String.valueOf(Math.round(60.0f / granularityRange));
                break;
            case HOUR:
                w1 = String.valueOf(Math.round(24.0f / granularityRange));
                w2 = String.valueOf(Math.round(168.0f / granularityRange));
                break;
            case DAY:
                w2 = String.valueOf(Math.round(7.0f / granularityRange));
                break;
            case WEEK:
                w2 = String.valueOf(Math.round(4.0f / granularityRange));
                break;
            case MONTH:
                w2 = String.valueOf(Math.round(12.0f / granularityRange));
                p.setPeriod("-1");
                break;
            default:
                break;
        }
        String baseWindows = w1 + Constants.COMMA_DELIMITER + w2;
        p.setBaseWindows(baseWindows);
        log.info("Updated BASE_WINDOWS: {}", baseWindows);
    }

    /**
     * Method to call egads.
     *
     * @param timeseries     input timeseries
     * @param sigmaThreshold Threshold for standard deviation
     * @return list of detected anomalies
     * @throws SherlockException exception in egads API
     */
    @SuppressWarnings("unchecked")
    public List<Anomaly> runEGADS(TimeSeries timeseries, Double sigmaThreshold) throws SherlockException {
        // list to store anomalies
        List<Anomaly> anomalies;
        log.debug("Call to egads API for sigma [{}] and timeseries [{}]", sigmaThreshold, timeseries.meta.id);
        try {
            // detect anomalies
            anomalies = detectAnomalies(timeseries);
        } catch (Exception e) {
            log.error("Error in Egads!", e);
            throw new SherlockException(e.getMessage());
        }
        log.debug("Egads completed");
        return anomalies;
    }

    /**
     * Before-run EGADS configuration. Ensures that
     * there is an existing properties object
     * and updates the base windows based
     * on the time series.
     * @param sigmaThreshold the sigma threshold
     * @param granularity timeseries granularity
     * @param granularityRange granularity range to aggregate on
     */
    public void preRunConfigure(Double sigmaThreshold, Granularity granularity, Integer granularityRange) {
        if (p == null) {
            log.error("Egads properties have not been set! Attempting to load from file.");
            configureFromFile();
        }
        p.setAutoSensitivityStandardDeviation(sigmaThreshold.toString());
        updateBaseWindow(granularity, granularityRange);
    }

    /**
     * Configure the current EGADS service instance
     * with a provided configuration object.
     *
     * @param config EGADS configuration object
     */
    public void configureWith(EgadsConfig config) {
        p = config;
    }

    /**
     * Configure the EGADS service instance
     * with the default configuration.
     */
    public void configureWithDefault() {
        init();
    }

    /**
     * Configure the current EGADS service instance
     * with a properties defined in a configuration file
     */
    public void configureFromFile() {
        p = EgadsConfig.fromFile();
    }

    /**
     * Method to run egads api to detect anomalies.
     *
     * @param timeseries input timeseries
     * @return list of anomalies
     * @throws SherlockException exception
     */
    @SuppressWarnings("unchecked")
    protected List<Anomaly> detectAnomalies(TimeSeries timeseries) throws SherlockException {
        List<Anomaly> anomalies = new ArrayList<>();
        try {
            ProcessableObject processableObject = getEgadsProcessableObject(timeseries);
            processableObject.process();
            anomalies.addAll((ArrayList<Anomaly>) processableObject.result());
        } catch (Exception e) {
            log.error("Exception from EGADS!", e);
            throw new SherlockException(e.getMessage());
        }
        return anomalies;
    }

    /**
     * Run an anomaly detection job, returning the list of
     * anomalies and the forecasted sequence through
     * the method parameters.
     *
     * @param timeseries the time series to analyze
     * @return an EGADS result object containing the above
     * @throws SherlockException if an error occurs during detection
     */
    @SuppressWarnings("unchecked")
    public EgadsResult detectAnomaliesResult(TimeSeries timeseries) throws SherlockException {
        try {
            // For now, instant query will show all anomalies on the graph
            p.setMaxAnomalyTimeAgo("99999999");
            ProcessableObject processableObject = getEgadsProcessableObject(timeseries);
            processableObject.process();
            List<Anomaly> anomalies = (List<Anomaly>) processableObject.result();
            ModelAdapter modelAdapter = EgadsUtils.getModelAdapter(processableObject);
            List<TimeSeries.DataSequence> expected = EgadsUtils.getModelForecast(timeseries, modelAdapter);
            return new EgadsResult(anomalies, timeseries, expected.get(0));
        } catch (Exception e) {
            log.error("Error in EGADS!", e);
            throw new SherlockException(e.getMessage(), e);
        }
    }

    /**
     * Mehtod to get processable object from egads.
     *
     * @param timeseries input timeseries
     * @return ProcessableObject instance
     */
    protected ProcessableObject getEgadsProcessableObject(TimeSeries timeseries) {
        return ProcessableObjectFactory.create(EgadsUtils.fillMissingData(timeseries, p), p.asProperties());
    }

    /**
     * Method to set the detection window for anomalies in the given job.
     * @param endTimeMinutes last datapoint timestamp in timeseries
     * @param frequency frequency of the job
     * @param nLookBack number of frequency to lookback
     */
    public void configureDetectionWindow(Integer endTimeMinutes, String frequency, int nLookBack) {
        Long detectionStartTime = (endTimeMinutes - nLookBack * Granularity.getValue(frequency).getMinutes()) * 60L;
        p.setDetectionWindowStartTime(detectionStartTime.toString());
    }
}
