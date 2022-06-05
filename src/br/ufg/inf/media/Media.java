package br.ufg.inf.media;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Collector;

import java.math.RoundingMode;
import java.text.DecimalFormat;

public class Media {

    private static final DecimalFormat df = new DecimalFormat("0.00");

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // Setting up rabbitmq's configurations;
        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("localhost").setPort(5672).setUserName("admin")
                .setPassword("admin").setVirtualHost("/").build();

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> nums = env.addSource(new RMQSource<String>(
                        connectionConfig,            // config for the RabbitMQ connection
                        "task_queue_input",                 // name of the RabbitMQ queue to consume
                        true,                        // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema()))   // deserialization schema to turn messages into Java objects
                .setParallelism(1);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> media = nums
                .map(new MapStrTuple())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceTuple()).map(new MapAvg());


        media.addSink(new RMQSink<String>(
                connectionConfig,            // config for the RabbitMQ connection
                "task_queue_output",                 // name of the RabbitMQ queue to send messages to
                new SimpleStringSchema()));

        // execute program
        env.executeAsync("media");

    }

    private static class MapStrTuple implements MapFunction<String, Tuple3<String, Double, Long>> {

        @Override
        public Tuple3<String, Double, Long> map(String s) throws Exception {
            String temp = s.substring(s.lastIndexOf(' ') + 1).trim();
            Double temperatura = Double.parseDouble(temp);
            String bairro = s.replaceAll(temp, "");
            bairro = bairro.trim();
            return new Tuple3<>(bairro,temperatura, 1L);
        }
    }

    private static class MapAvg implements MapFunction<Tuple3<String, Double, Long>, String> {
        @Override
        public String map(Tuple3<String, Double, Long> t) throws Exception {
            Double media = t.f1 / (double) t.f2;
            String resultado = t.f0 + ';' + df.format(media);
            return resultado;
        }
    }

    private static class ReduceTuple implements ReduceFunction<Tuple3<String, Double, Long>> {
        @Override
        public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> v0, Tuple3<String, Double, Long> v1) throws Exception {
            return new Tuple3<>(v0.f0, v0.f1 + v1.f1, v0.f2 + v1.f2);
        }
    }
}
