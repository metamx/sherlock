/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

import com.yahoo.egads.data.TimeSeries;
import com.yahoo.sherlock.query.EgadsConfig;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.util.Date;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

/**
 * Test class for missing data filling util in egads.
 */
@Slf4j
public class EgadsUtilsTest {

    /**
     * Method to test fillMissingData().
     *
     * @throws Exception exception
     */
    @Test
    public void testFillMissingData() throws Exception {
        TimeSeries series = new TimeSeries();
        series.append(3600L, 11.11f);
        series.append(7200L, 22.22f);
        series.append(10800L, 33.33f);
        series.append(14400L, 55.55f);
        series.append(18000L, 33.33f);
        series.append(21600L, 77.77f);
        series.append(25200L, 99.99f);
        series.append(36000L, 100.99f);
        EgadsConfig p = new EgadsConfig();
        p.setAggregation("1");
        p.setFillMissing("1");
        TimeSeries ts = EgadsUtils.fillMissingData(series, p);
        ts.data.forEach(datapoint -> {
                Timestamp stamp = new Timestamp(datapoint.time * 1000);
                Date date = new Date(stamp.getTime());
                log.info(date.toString() + "  " + datapoint.value);
            }
        );
    }

    @Test
    public void testFillMissingDataUnequalPeriodFrequency() {
        TimeSeries ts = mock(TimeSeries.class);
        when(ts.mostFrequentPeriod()).thenReturn(10L);
        when(ts.minimumPeriod()).thenReturn(100L);
        assertEquals(ts, EgadsUtils.fillMissingData(ts, 1, 1));
    }

}
