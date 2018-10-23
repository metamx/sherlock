/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.yahoo.sherlock.enums.JobStatus;
import com.yahoo.sherlock.exception.DruidException;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.model.EgadsResult;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.model.DruidCluster;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.query.EgadsConfig;
import com.yahoo.sherlock.query.Query;
import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.TimeSeries;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Service class for anomaly detection.
 */
@Slf4j
public class DetectorService {

    /**
     * Class service factory instance.
     */
    private ServiceFactory serviceFactory = new ServiceFactory();

    /**
     * Class druid query service instance.
     */
    private DruidQueryService queryService = serviceFactory.newDruidQueryServiceInstance();

    /**
     * Class HTTP service instance.
     */
    private HttpService httpService = serviceFactory.newHttpServiceInstance();

    /**
     * Class time series parser service instance.
     */
    private TimeSeriesParserService parserService = serviceFactory.newTimeSeriesParserServiceInstance();

    /**
     * Class Egads config loaded from file or with default values if file is not found.
     */
    private EgadsConfig egadsConfig = EgadsConfig.fromFile();

    /**
     * Empty constructor.
     */
    public DetectorService() {
    }

    /**
     * Method to detect anomalies.
     * This method handles the control/data flow between the components of detection system.
     *
     * @param cluster           the Druid query to issue the query
     * @param jobMetadata       job metadata
     * @return list of anomalies
     * @throws SherlockException exeption thrown while runnig the anomaly detector components
     * @throws DruidException    if an error querying druid occurs
     */
    public List<Anomaly> detect(
        DruidCluster cluster,
        JobMetadata jobMetadata
    ) throws SherlockException, DruidException {
        Granularity granularity = Granularity.getValue(jobMetadata.getGranularity());
        Query query = queryService.build(jobMetadata.getQuery(), granularity, jobMetadata.getGranularityRange(), jobMetadata.getEffectiveQueryTime(), jobMetadata.getTimeseriesRange());
        log.info("Query generation successful.");
        return detect(query, cluster, jobMetadata);
    }

    /**
     * Perform a detection job with a specified query
     * on a cluster.
     *
     * @param query            the query to use
     * @param cluster          the cluster to query
     * @param job              the job meta data
     * @return a list of anomalies
     * @throws SherlockException if an error occurs during detection
     * @throws DruidException    if an error occurs while contacting Druid
     */
    public List<Anomaly> detect(
        Query query,
        DruidCluster cluster,
        JobMetadata job
    ) throws SherlockException, DruidException {
        checkDatasource(query, cluster);
        JsonArray druidResponse = queryDruid(query, cluster);
        return runDetection(druidResponse, query, job);
    }

    /**
     * Check to ensure that the datasource in the query exists
     * in the specified cluster.
     *
     * @param query   the query to check
     * @param cluster the druid cluster to check
     * @throws DruidException if the datasource is not found
     */
    public void checkDatasource(Query query, DruidCluster cluster) throws DruidException {
        JsonElement datasource = query.getDatasource();
        JsonArray druidDatasources = httpService.queryDruidDatasources(cluster);
        if (!druidDatasources.contains(datasource)) {
            log.error("Druid datasource {} does not exist!", datasource);
            throw new DruidException("Querying unknown datasource: " + datasource);
        }
    }

    /**
     * Send the query to druid and return the parsed JSON array
     * response to the caller.
     *
     * @param query   the query to execute
     * @param cluster the cluster to query
     * @return the parsed response
     * @throws DruidException if an error occurs while calling druid
     */
    public JsonArray queryDruid(Query query, DruidCluster cluster) throws DruidException {
        JsonArray druidResponse = httpService.queryDruid(cluster, query.getQueryJsonObject());
        log.info("Druid response received successfully");
        log.debug("Response from Druid is: {}", druidResponse);
        if (druidResponse.size() == 0) {
            log.error("Query to Druid returned empty response!");
        }
        return druidResponse;
    }

    /**
     * Run detection with a provided EGADS configuration and
     * Druid query.
     *
     * @param druidResponse    response from Druid
     * @param query            the Druid query
     * @param jobMetadata      the job meta data
     * @return anomalies from detection
     * @throws SherlockException if an error occurs during analysis
     */
    public List<Anomaly> runDetection(
            JsonArray druidResponse,
            Query query,
            JobMetadata jobMetadata
    ) throws SherlockException {
        List<TimeSeries> timeSeriesList = parserService.parseTimeSeries(druidResponse, query);
        // The value of the last timestamp expected to be returned by Druid
        Integer granularityRange = jobMetadata.getGranularityRange();
        Integer expectedEnd = (query.getRunTime() / 60) - (query.getGranularity().getMinutes() * granularityRange);
        List<Anomaly> anomalies = runDetection(timeSeriesList, jobMetadata, expectedEnd, query.getGranularity());
        log.info("Generated anomaly list with {} anomalies", anomalies.size());
        return anomalies;
    }

    /**
     * Run detection on a list of time series.
     *
     * @param timeSeriesList   time series to analyze
     * @param jobMetadata      job meta data
     * @param endTimeMinutes   the expected last data point time in minutes
     * @param granularity      the granularity
     * @return list of anomalies from the detection job
     * @throws SherlockException if an error occurs during analysis
     */
    public synchronized List<Anomaly> runDetection(
            List<TimeSeries> timeSeriesList,
            JobMetadata jobMetadata,
            Integer endTimeMinutes,
            Granularity granularity
    ) throws SherlockException {
        Integer granularityRange = jobMetadata.getGranularityRange();
        Double sigmaThreshold = jobMetadata.getSigmaThreshold();
        String frequency = jobMetadata.getFrequency();

        EgadsService egadsService = serviceFactory.newEgadsServiceInstance();
        egadsService.configureWith(egadsConfig);
        egadsService.getP().setTsModel(jobMetadata.getTimeseriesModel());
        egadsService.getP().setAdModel(jobMetadata.getAnomalyDetectionModel());
        egadsService.preRunConfigure(sigmaThreshold, granularity, granularityRange);
        // Configure the detection window for anomaly detection
        egadsService.configureDetectionWindow(endTimeMinutes, frequency, granularityRange);

        List<Anomaly> anomalies = new ArrayList<>(timeSeriesList.size());
        for (TimeSeries timeSeries : timeSeriesList) {
            if (timeSeries.data.isEmpty() ||
                timeSeries.data.get(timeSeries.data.size() - 1).time != endTimeMinutes * 60L) {
                anomalies.add(getNoDataAnomaly(timeSeries, egadsService.getP().getAdModel()));
            } else {
                anomalies.addAll(egadsService.runEGADS(timeSeries, sigmaThreshold));
            }
        }
        return anomalies;
    }

    /**
     * @param timeSeries time series for which to generate empty anomaly
     * @return an anomaly that represents no data
     */
    private Anomaly getNoDataAnomaly(TimeSeries timeSeries, String modelName) {
        Anomaly anomaly = new Anomaly();
        anomaly.metricMetaData.name = JobStatus.NODATA.getValue();
        anomaly.metricMetaData.source = timeSeries.meta.source;
        anomaly.id = timeSeries.meta.id;
        anomaly.intervals = new Anomaly.IntervalSequence();
        anomaly.modelName = modelName;
        return anomaly;
    }


    /**
     * Perform an egads detection and return the results
     * as an {@code EgadsResult}.
     *
     * @param query           druid query
     * @param sigmaThreshold  sigma threshold to use
     * @param cluster         the druid cluster to query
     * @param detectionWindow detection window for anomalies
     * @param config          the egads configuration
     * @return a list of egads results
     * @throws SherlockException if an error during processing occurs
     * @throws DruidException    if an error during querying occurs
     */
    public List<EgadsResult> detectWithResults(
            Query query,
            Double sigmaThreshold,
            DruidCluster cluster,
            @Nullable Integer detectionWindow,
            @Nullable EgadsConfig config
    ) throws SherlockException, DruidException {
        checkDatasource(query, cluster);
        JsonArray druidResponse = queryDruid(query, cluster);
        List<TimeSeries> timeSeriesList = parserService.parseTimeSeries(druidResponse, query);
        List<EgadsResult> results = new ArrayList<>(timeSeriesList.size());

        final EgadsService egadsService = serviceFactory.newEgadsServiceInstance();
        egadsService.configureWith(config);
        egadsService.preRunConfigure(sigmaThreshold, query.getGranularity(), query.getGranularityRange());
        if (detectionWindow != null) {
            egadsService.configureDetectionWindow(query.getRunTime() / 60, query.getGranularity().toString(), detectionWindow + 1);
        }

        for (TimeSeries timeSeries : timeSeriesList) {
            results.add(egadsService.detectAnomaliesResult(timeSeries));
        }
        return results;
    }
}
