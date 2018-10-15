package com.yahoo.egads.models.tsmm;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.egads.data.TimeSeries;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.mortbay.jetty.HttpMethods;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

/**
 * Prophet (by Facebook) forecasting model.
 */
@Slf4j
public class ProphetModel extends TimeSeriesAbstractModel {

  /**
   * Prophet service url.
   */
  private final String url;

  /**
   * Data sequence to train the model.
   */
  private TimeSeries.DataSequence dataSequence;

  /**
   * Prophet URL config property name.
   */
  public static final String PROPHET_URL = "PROPHET_URL";

  public ProphetModel(Properties config)
  {
    super(config);
    url = config.getProperty(PROPHET_URL);
    System.out.println("Prophet URL: " + url);
  }

  /**
   * Train the model.
   * @param dataSequence data sequence to train the model
   * @throws Exception
   */
  @Override
  public void train(TimeSeries.DataSequence dataSequence) throws Exception
  {
    this.dataSequence = dataSequence;
  }

  /**
   * Update the data sequence to train the model.
   * @param dataSequence
   * @throws Exception
   */
  @Override
  public void update(TimeSeries.DataSequence dataSequence) throws Exception
  {
    this.dataSequence = dataSequence;
  }

  /**
   * Predict and fill data sequence.
   * @param dataSequence data sequence to be filled
   * @throws Exception
   */
  @Override
  public void predict(TimeSeries.DataSequence dataSequence) throws Exception
  {
    if (this.dataSequence == null || this.dataSequence.isEmpty() || dataSequence == null || dataSequence.isEmpty()) {
      return;
    }

    final HttpURLConnection connection = (HttpURLConnection) (new URL(url).openConnection());
    connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
    connection.setRequestMethod(HttpMethods.POST);
    connection.setDoOutput(true);
    connection.setDoInput(true);
    final ObjectMapper objectMapper = new ObjectMapper();
    try(final OutputStream os = connection.getOutputStream()) {
      objectMapper.writeValue(os, this.dataSequence);
    }
    final String responseJson = IOUtils.toString(connection.getInputStream(), StandardCharsets.UTF_8.name());
    final List<TimeSeries.Entry> response =
        objectMapper.readValue(responseJson, new TypeReference<List<TimeSeries.Entry>>(){});
    for (int i = 0; i < dataSequence.size(); i++) {
      dataSequence.set(i, response.get(i));
    }
  }

  /**
   * Reset the model by emptying the data sequence.
   */
  @Override
  public void reset()
  {
    dataSequence.clear();
  }
}
