package com.yahoo.sherlock.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dikhan.pagerduty.client.events.PagerDutyEventsClient;
import com.github.dikhan.pagerduty.client.events.domain.Payload;
import com.github.dikhan.pagerduty.client.events.domain.Severity;
import com.github.dikhan.pagerduty.client.events.domain.TriggerIncident;
import com.github.dikhan.pagerduty.client.events.exceptions.NotifyEventException;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.Constants;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

import java.time.OffsetDateTime;
import java.util.List;


/**
 * The service for sending pagers to the PagerDuty
 */
@Slf4j
public class PagerDutyService
{

  public boolean sendPager(String pagerKeys, List<AnomalyReport> anomalies) {

    PagerDutyEventsClient pagerDutyEventsClient = PagerDutyEventsClient.create();

    for (String pagerKey : pagerKeys.split(Constants.COMMA_DELIMITER)) {
      for (AnomalyReport report : anomalies) {
        String sherlockLink = String.format(
            "http://sherlock.metamx.com:%d/Reports/%s/%s",
            CLISettings.PORT,
            report.getJobId(),
            report.getJobFrequency()
        );
        final ObjectMapper objectMapper = new ObjectMapper();
        try {
          Payload payload = Payload.Builder
              .newBuilder()
              .setSummary(String.format("Anomaly: %s", report.getTestName()))
              .setSource(report.getTestName())
              .setSeverity(Severity.INFO)
              .setTimestamp(OffsetDateTime.now())
              .setCustomDetails(new JSONObject(report))
              .build();
          TriggerIncident incident = TriggerIncident.TriggerIncidentBuilder
              .newBuilder(pagerKey, payload)
              .setClientUrl(sherlockLink)
              .setClient("Sherlock")
              .build();
          pagerDutyEventsClient.trigger(incident);
        }
        catch (NotifyEventException e) {
          log.error(String.format("Error while triggering a pager using [%s] as a service key", pagerKey), e);
        }
      }
    }
    return true;
  }
}
