/**
 * Put your copyright and license info here.
 */
package com.example.mydtapp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.IdempotentStorageManager;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build


    KafkaSinglePortStringInputOperator input = dag.addOperator("KafkaInput", KafkaSinglePortStringInputOperator.class);

    input.setIdempotentStorageManager(new IdempotentStorageManager.FSIdempotentStorageManager());

    input.setInitialOffset("earliest");

    input.getConsumer().setZookeeper("localhost:2181");
    input.getConsumer().setTopic("develop");


    //RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);
    //randomGenerator.setNumTuples(500);

    ConsoleOutputOperator cons = dag.addOperator("console", new ConsoleOutputOperator());

    dag.addStream("randomData", input.outputPort, cons.input).setLocality(Locality.CONTAINER_LOCAL);
  }
}
