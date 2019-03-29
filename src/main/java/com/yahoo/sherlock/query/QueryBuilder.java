/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.query;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.yahoo.sherlock.utils.NumberUtils;
import com.yahoo.sherlock.utils.TimeUtils;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.settings.QueryConstants;

import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Stack;

/**
 * Query build class enables builder syntax for constructing
 * queries. Provides default settings for queries but also
 * enables caller to provide more specific details for the query.
 */
@Slf4j
public class QueryBuilder {

    /**
     * Start building a new query.
     *
     * @return a query builder instance
     */
    public static QueryBuilder start() {
        return new QueryBuilder();
    }

    /**
     * The raw druid query string.
     */
    private String queryString;
    /**
     * The query end time.
     */
    private ZonedDateTime endTime;
    /**
     * The query start time.
     */
    private ZonedDateTime startTime;
    /**
     * The query time granularity.
     */
    private Granularity granularity;
    /**
     * The number of intervals of granularity
     * before end time that should be used to
     * find the start time. This parameter
     * takes precedence or start time.
     */
    private int intervals;

    /**
     * The start position of the left curly of
     * the import query in the query string.
     */
    private Integer queryStartPos;
    /**
     * The end poistion of the right curly
     * of the important query.
     */
    private Integer queryEndPos;

    /**
     * Range of granularity to aggregate on.
     */
    private Integer granularityRange;

    /**
     * Boolean to indicate the query is for backfill or not.
     */
    private boolean isBackFillQuery = false;

    /**
     * Private query constructor initializes all
     * values to invalid.
     */
    private QueryBuilder() {
        queryString = null;
        endTime = null;
        startTime = null;
        granularity = null;
        intervals = -1;
        queryStartPos = null;
        queryEndPos = null;
        granularityRange = 1;
    }

    /**
     * @param queryString query string to set
     * @return query builder instance
     */
    public QueryBuilder queryString(String queryString) {
        this.queryString = queryString;
        return this;
    }

    /**
     * Set the end time to be the current time in UTC.
     * This is the default setting for end time.
     *
     * @return query builder instance
     */
    public QueryBuilder endNow() {
        this.endTime = ZonedDateTime.now(ZoneOffset.UTC);
        return this;
    }

    /**
     * Specify the end time to be a specific time.
     *
     * @param endTime the desired query end time
     * @return query builder instance
     */
    public QueryBuilder endAt(ZonedDateTime endTime) {
        this.endTime = endTime;
        return this;
    }

    /**
     * Set the end time from a time string.
     * @param endTimeStr end time string
     * @return query builder instance
     */
    public QueryBuilder endAt(String endTimeStr) {
        this.endTime = TimeUtils.parseDateTime(endTimeStr);
        return this;
    }

    /**
     * Set the end time from a given endTime in minutes.
     * @param endTime end time of interval
     * @return query builder instance
     */
    public QueryBuilder endAt(Integer endTime) {
        this.endTime = (endTime != null ? ZonedDateTime.ofInstant(Instant.ofEpochSecond(endTime * 60), ZoneOffset.UTC) : null);
        return this;
    }

    /**
     * Set the start time from a given startTime in minutes.
     * @param startTime start time of interval
     * @return query builder instance
     */
    public QueryBuilder startAt(Integer startTime) {
        this.endTime = (endTime != null ? ZonedDateTime.ofInstant(Instant.ofEpochSecond(startTime * 60), ZoneOffset.UTC) : null);
        return this;
    }

    /**
     * Specify the start time to be a specific time.
     *
     * @param startTime the desired query start time
     * @return query builder instance
     */
    public QueryBuilder startAt(ZonedDateTime startTime) {
        this.startTime = startTime;
        return this;
    }

    /**
     * Set the start time from a date string.
     * @param startTimeStr start time string
     * @return query builder instance
     */
    public QueryBuilder startAt(String startTimeStr) {
        this.startTime = TimeUtils.parseDateTime(startTimeStr);
        return this;
    }

    /**
     * @param granularity the query granularity to set
     * @return query builder instance
     */
    public QueryBuilder granularity(Granularity granularity) {
        this.granularity = granularity;
        return this;
    }

    /**
     * Set the granularity from a string value
     * using {@code Granularity.getValue}.
     * @param granularityStr granularity string value
     * @return query builder instance
     */
    public QueryBuilder granularity(String granularityStr) {
        this.granularity = Granularity.getValue(granularityStr);
        return this;
    }

    /**
     * @return the query granularity
     */
    public Granularity getGranularity() {
        return granularity;
    }

    /**
     * Set the number of intervals of granularity before the
     * end time for the start time.
     *
     * @param intervals number of intervals
     * @return query builder instance
     */
    public QueryBuilder intervals(Integer intervals) {
        if (intervals != null) {
            this.intervals = intervals;
        }
        return this;
    }

    /**
     * Set the number of intervals from a string value.
     * @param intervalsStr intervals string value
     * @return query builder instance
     */
    public QueryBuilder intervals(String intervalsStr) {
        if (!NumberUtils.isInteger(intervalsStr)) {
            return this;
        }
        int intervals = Integer.parseInt(intervalsStr);
        if (intervals >= 0) {
            this.intervals = intervals;
        }
        return this;
    }

    /**
     * Set the granularityRange to aggregate on.
     * @param granularityRange granularity range value
     * @return query builder instance
     */
    public QueryBuilder granularityRange(Integer granularityRange) {
        if (granularityRange == null || granularityRange < 1) {
            return this;
        }
        this.granularityRange = granularityRange;
        return this;
    }

    /**
     * @return the number of intervals in the query
     */
    public int getIntervals() {
        return intervals;
    }

    /**
     * @return query builder instance
     */
    public QueryBuilder setIsBackFillQuery(boolean isBackFillQuery) {
        this.isBackFillQuery = isBackFillQuery;
        return this;
    }

    /**
     * Find the start and end position of the phase 2 query, if it exists,
     * in the query string. Also performs basic JSON syntax validation.
     *
     * @return true if the query is valid, false otherwise
     */
    protected boolean findQueryPosition() {
        Stack<Integer> stack = new Stack<>();
        char[] chars = queryString.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];
            if (c != '{' && c != '}') {
                continue;
            }
            if (c == '{') {
                stack.push(i);
                continue;
            }
            if (!stack.empty()) {
                queryStartPos = stack.peek();
                queryEndPos = i + 1;
                stack.pop();
            } else {
                return false;
            }
        }
        return queryStartPos != null && queryEndPos != null;
    }

    /**
     * Preprocess the query parameters.
     *
     * @throws SherlockException if an error is found in the parameters
     */
    protected void preBuild() throws SherlockException {
        if (queryString == null || queryString.isEmpty()) {
            // Query string is required
            throw new SherlockException("Empty query string provided");
        }
        if (granularity == null) {
            // Default granularity is day
            granularity = Granularity.DAY;
        }
        if (endTime == null) {
            // End now if not specified
            endNow();
        }
        if (startTime == null && intervals < 0) {
            // Get the default intervals for the granularity
            // if no indication of start time was given
            intervals = granularity.getIntervalsFromSettings();
        }
        if (intervals >= 0) {
            // Prefer intervals over start time
            startTime = granularity.subtractIntervals(endTime, intervals, granularityRange);
        }
        if (!findQueryPosition()) {
            throw new SherlockException("Invalid query syntax! Check JSON brackets");
        }
        queryString = queryString.substring(queryStartPos, queryEndPos);
    }

    /**
     * Convert the query string to a JSON object.
     *
     * @param queryString druid query string
     * @return druid query as a JSON object
     * @throws SherlockException if an error parsing the JSON occurs
     */
    protected static JsonObject toJson(String queryString) throws SherlockException {
        try {
            return new Gson().fromJson(queryString, JsonObject.class);
        } catch (Exception e) {
            throw new SherlockException("Parsing query json failed!");
        }
    }

    /**
     * Ensure that the query json object has the necessary propeties.
     *
     * @param object query json object to validate
     * @throws SherlockException if some parameters are missing
     */
    protected static void validateJsonObject(JsonObject object) throws SherlockException {
        if (!(object.has(QueryConstants.AGGREGATIONS) &&
              object.has(QueryConstants.INTERVALS) &&
              object.has(QueryConstants.GRANULARITY))) {
            throw new SherlockException("Druid query is missing parameters");
        }
    }

    /**
     * @param date zoned date time object to format
     * @return a date formatted as a druid date
     */
    public static String asDruidDate(ZonedDateTime date) {
        if (date == null) {
            return null;
        }
        String preq = date.toString().split(QueryConstants.DATE_TIME_SPLIT)[0];
        if (preq.charAt(preq.length() - 1) == 'Z') {
            preq = preq.substring(0, preq.length() - 1);
        }
        return preq + QueryConstants.DATE_TIME_ZERO;
    }

    /**
     * @param date zoned date time object to format
     * @return a date formatted as a druid origin data
     */
    public static String asDruidOrigin(ZonedDateTime date) {
        if (date == null) {
            return null;
        }
        String preq = date.toString().split(QueryConstants.DATE_TIME_SPLIT)[0];
        if (preq.charAt(preq.length() - 1) == 'Z') {
            preq = preq.substring(0, preq.length() - 1);
        }
        return preq;
    }

    /**
     * Get the start time and end time as a query interval
     * in druid time format.
     *
     * @param startTime query start time
     * @param endTime   query end time
     * @return druid interval string
     */
    protected static String getInterval(ZonedDateTime startTime, ZonedDateTime endTime) {
        return asDruidDate(startTime) + '/' + asDruidDate(endTime);
    }

    /**
     * @param object   query json object whose interval to set
     * @param interval druid time interval string
     */
    protected static void setObjectInterval(JsonObject object, String interval) {
        object.remove(QueryConstants.INTERVALS);
        object.addProperty(QueryConstants.INTERVALS, interval);
    }

    /**
     * @param object query json object whose period to set
     * @param period granularity to set
     * @param granularityRange granularity range to aggregate on
     * @param startTime start time of query
     */
    protected static void setObjectGranularity(JsonObject object, String period, Integer granularityRange, ZonedDateTime startTime, Boolean isBackFillQuery) {
        period = isBackFillQuery ? period : period.replace("1", granularityRange.toString());
        object.remove(QueryConstants.GRANULARITY);
        JsonObject granularityObj = new JsonObject();
        granularityObj.addProperty(QueryConstants.TYPE, QueryConstants.PERIOD);
        granularityObj.addProperty(QueryConstants.PERIOD, period);
        granularityObj.addProperty(QueryConstants.TIMEZONE, QueryConstants.UTC);
        granularityObj.addProperty(QueryConstants.ORIGIN, asDruidOrigin(startTime));
        object.add(QueryConstants.GRANULARITY, granularityObj);
    }

  /**
   * @param object query json object whose context to update
   * @param metaType meta type of the query
   * @param userId meta user id of the query
   */
    protected void setObjectContextMetaTypeAndUserId(JsonObject object, String metaType, String userId) {
      JsonObject context = object.getAsJsonObject(QueryConstants.CONTEXT);
      if (context == null) {
        context = new JsonObject();
        object.add(QueryConstants.CONTEXT, context);
      }
      JsonObject meta = context.getAsJsonObject(QueryConstants.META_DATA);
      if (meta == null) {
        meta = new JsonObject();
        context.add(QueryConstants.META_DATA, meta);
      }
      if (meta.get(QueryConstants.META_TYPE) == null) {
        meta.addProperty(QueryConstants.META_TYPE, metaType);
      }
      if (meta.get(QueryConstants.USER_ID) == null) {
        meta.addProperty(QueryConstants.USER_ID, userId);
      }
    }

    /**
     * @return the build Query object
     * @throws SherlockException if an error occurs while building
     */
    public Query build() throws SherlockException {
        this.preBuild();
        JsonObject queryObj = toJson(queryString);
        validateJsonObject(queryObj);
        setObjectInterval(queryObj, getInterval(startTime, endTime));
        setObjectGranularity(queryObj, granularity.getValue(), granularityRange, startTime, isBackFillQuery);
        setObjectContextMetaTypeAndUserId(queryObj, QueryConstants.META_TYPE_SHERLOCK, QueryConstants.META_USER_ID);
        return new Query(queryObj, getStartTime(), getRunTime(), granularity, granularityRange);
    }

    /**
     * Get the effective runTime of the query, which
     * will be at the end time.
     * @return runTime timestamp value in seconds
     */
    public Integer getRunTime() {
        return (int) (endTime.toInstant().toEpochMilli() / 1000L);
    }

    /**
     * Get the effective start time of the query.
     * @return start time timestamp value in seconds
     */
    public Integer getStartTime() {
        return (int) (startTime.toInstant().toEpochMilli() / 1000L);
    }

}
