package br.ufg.inf.media;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

public class Media {

    public static final String[] NUMS = new String[] {"10", "20", "30", "40", "50", "60"};

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

      //  DataSet<String> nums = env.fromElements(NUMS);
       DataStream<String> nums = env.readTextFile("file:///home/beatriz/data.txt");

       // nums.map(new MapFunction()).groupBy(2).reduceGroup(new ReduceGroup()).reduce(new ReduceTuple()).map(new MapAvg()).print();
        nums.map(new MapStrTuple()).groupBy(2).reduce(new ReduceTuple()).map(new MapAvg()).print();
    }

    private static class MapIntTuple implements MapFunction<Long, Tuple3<Long, Long, Long>> {
        private long key = 2L;
        @Override
        public Tuple3<Long, Long, Long> map(Long n) throws Exception {
            key = (key + 1L) % 3L;
            return new Tuple3<Long, Long, Long>(n, 1L, key);
        }
    }

    private static class MapStrTuple implements MapFunction<String, Tuple3<Long, Long, Long>> {
        private long key = 2L;
        @Override
        public Tuple3<Long, Long, Long> map(String s) throws Exception {
            key = (key + 1L) % 3L;
            return new Tuple3<>(Long.parseLong(s), 1L, key);
        }
    }

    private static class MapAvg implements MapFunction<Tuple3<Long, Long, Long>, Double> {
        @Override
        public Double map(Tuple3<Long, Long, Long> t) throws Exception {
            return (double)t.f0 / (double)t.f1;
        }
    }

    private static class ReduceTuple implements ReduceFunction<Tuple3<Long, Long, Long>> {
        @Override
        public Tuple3<Long, Long, Long> reduce(Tuple3<Long, Long, Long> v0, Tuple3<Long, Long, Long> v1) throws Exception {
            return new Tuple3<>(v0.f0+v1.f0, v0.f1+v1.f1, 0L);
        }
    }

    private static class ReduceGroup implements GroupReduceFunction<Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>>{
        @Override
        public void reduce(Iterable<Tuple3<Long, Long, Long>> iter, Collector<Tuple3<Long, Long, Long>> out) throws Exception {
            Tuple3<Long, Long, Long> resp = new Tuple3<>(0L, 0L, 0L);
            for (Tuple3<Long, Long, Long> t : iter) {
                resp.f0 = resp.f0 + t.f0;
                resp.f1 = resp.f1 + t.f1;
            }
            out.collect(resp);
        }
    }
    final EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
  //  final StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(configuration, fsSettings);
   // DataStream<String> finalRes = fsTableEnv.toAppendStream(tableNameHere, Media.class);


}
