package com.hiroshisilva.favourite.colours.kafka.streams

import java.lang
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KTable}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}

object FavouriteColour {


  def main(args: Array[String])  {

    val config: Properties = new Properties
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    val intermediaryTopic = "favourite-colour-intermediate-topic"


    val kStreamBuilder = new KStreamBuilder()
    val textLine :KStream[String, String] = kStreamBuilder.stream[String,String]("favourite-colour-input")

    textLine
      .filter((key,value) => value.contains(","))
      .selectKey((key,values) => values.split(",")(0).toLowerCase)
      .mapValues(_.split(",")(1).toLowerCase)
      .filter((key,value)=> List("red","blue","green").contains(value))
      .to(intermediaryTopic)

    val coloursKTable = kStreamBuilder.table[String,String](intermediaryTopic)

    val favouriteColours: KTable[String, lang.Long] =
      coloursKTable
        .groupBy((key,value) => new KeyValue[String, String](value,value))
        .count("CountsByColours")


    favouriteColours.to(Serdes.String, Serdes.Long,"favourite-colour-output")


    val streams = new KafkaStreams(kStreamBuilder,config)
    streams.start()

    System.out.println(streams.toString)


    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        streams.close()
      }
    })





  }


}
