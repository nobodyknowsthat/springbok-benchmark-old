package com.anonymous.test.benchmark.benchmark;

import com.anonymous.test.benchmark.predicate.QueryGenerator;
import com.anonymous.test.benchmark.predicate.StatisticUtil;
import com.anonymous.test.benchmark.springbok.SpringbokDriver;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author anonymous
 * @create 2022-06-28 2:32 PM
 **/
public class SpringbokSpatialTemporalBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        @Param({"24h"})
        public String timeLength;

        //@Param({"001", "01", "1"})
        @Param({"01"})
        public String spatialWidth;

        String queryFilenamePrefix = "/home/anonymous/Data/DataSet/Trajectory/TaxiPorto/archive/query-on-10w/porto_10w_";

        List<SpatialTemporalRangeQueryPredicate> predicateList;

        @Setup(Level.Trial)
        public void setup() {
            // set query
            String queryFilename = queryFilenamePrefix + timeLength + "_" + spatialWidth + ".query";
            predicateList = QueryGenerator.getSpatialTemporalRangeQueriesFromQueryFile(queryFilename);
        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 0, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(50)    // the number of queries
    @Measurement(time = 5, iterations = 1)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void spatialTemporalRangeQuery(Blackhole blackhole, BenchmarkState state) {
        List<Integer> resultCountList = new ArrayList<>();
        for (SpatialTemporalRangeQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            int resultCount = SpringbokDriver.spatialTemporalQuery(predicate);
            System.out.println("result count: " + resultCount);
            resultCountList.add(resultCount);
            blackhole.consume(resultCount);
        }
        System.out.println("average result count: " + StatisticUtil.calculateAverage(resultCountList));
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SpringbokSpatialTemporalBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }

}
