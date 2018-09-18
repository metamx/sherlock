package com.yahoo.sherlock.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dikhan.PagerDutyEventsClient;
import com.github.dikhan.domain.LinkContext;
import com.github.dikhan.domain.TriggerIncident;
import com.github.dikhan.exceptions.NotifyEventException;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.Constants;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The service for sending pagers to the PagerDuty
 */
@Slf4j
public class PagerDutyService
{
  public List<PagerKey> filterPagerKeys(String emailsAndPagerKeys) {
    return Arrays
        .stream(emailsAndPagerKeys.replace(" ", "").split(Constants.COMMA_DELIMITER))
        .filter(s -> !s.contains("@"))
        .map(PagerKey::new)
        .collect(Collectors.toList());
  }

  public boolean sendPager(String owner, List<PagerKey> pagerKeys, List<AnomalyReport> anomalies) {
    PagerDutyEventsClient pagerDutyEventsClient = PagerDutyEventsClient.create();

    pagerKeys.forEach(pagerKey -> {
      anomalies.forEach(report -> {
        String sherlockLink = String.format(
            "http://sherlock.metamx.com:$d/Reports/%s/%s",
            CLISettings.PORT,
            report.getJobId(),
            report.getJobFrequency()
        );
        final ObjectMapper objectMapper = new ObjectMapper();
        try {
          final String reportJson = objectMapper.writeValueAsString(report);
          TriggerIncident incident = TriggerIncident.TriggerIncidentBuilder
              .create(pagerKey.key, "Anomaly detected")
              .client("Sherlock")
              .clientUrl(sherlockLink)
              .contexts(Collections.singletonList(new LinkContext(sherlockLink)))
              .details(reportJson)
              .build();
          pagerDutyEventsClient.trigger(incident);
        }
        catch (JsonProcessingException e) {
          log.error(String.format("Error while generating a json for anomaly report [%s]", report), e);
        }
        catch (NotifyEventException e) {
          log.error(String.format("Error while triggering a pager using [%s] as a service key", pagerKey), e);
        }
      });
    });
    return true;
  }

  @AllArgsConstructor
  private class PagerKey {
    @NonNull
    private final String key;
  }
}
