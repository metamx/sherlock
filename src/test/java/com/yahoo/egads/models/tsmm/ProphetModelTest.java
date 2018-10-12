package com.yahoo.egads.models.tsmm;

import com.yahoo.egads.data.TimeSeries;
import org.junit.Test;

import java.util.Properties;

public class ProphetModelTest
{

  @Test
  public void predict() throws Exception
  {
    final Properties properties = new Properties();
    properties.setProperty(ProphetModel.PROPHET_URL, "http://localhost:9090");
    final ProphetModel prophetModel = new ProphetModel(properties);
    final TimeSeries.DataSequence predicted = new TimeSeries.DataSequence();
    final TimeSeries.DataSequence initial = new TimeSeries.DataSequence();
    for (int i = 0; i < 1000; i++) {
      initial.add(new TimeSeries.Entry(i, i));
    }
    prophetModel.train(initial);
    prophetModel.predict(predicted);
  }
}