package com.anonymous.test.benchmark.benchmark;

import com.anonymous.test.benchmark.predicate.QueryGenerator;
import com.anonymous.test.benchmark.predicate.StatisticUtil;
import com.anonymous.test.benchmark.springbok.SpringbokDriver;
import com.anonymous.test.index.predicate.IdTemporalQueryPredicate;
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
 * @create 2022-06-28 2:08 PM
 **/
public class SpringbokIdTemporalBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        @Param({"1h", "6h", "24h", "7d"})
        //@Param({"7d"})
        public String timeLength;

        String queryFilenamePrefix = "/home/anonymous/Data/DataSet/Trajectory/TaxiPorto/archive/query-fulldata/porto_fulldata_id_";

        List<IdTemporalQueryPredicate> predicateList;

        @Setup(Level.Trial)
        public void setup() {
            // set query
            String queryFilename = queryFilenamePrefix + timeLength + ".query";
            predicateList = QueryGenerator.getIdTemporalQueriesFromQueryFile(queryFilename);
        }

    }

    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(500)    // the number of queries
    @Measurement(time = 5, iterations = 3)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void idTemporalQuery(Blackhole blackhole, BenchmarkState state) {
        List<Integer> resultCountList = new ArrayList<>();
        for (IdTemporalQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            int resultCount = SpringbokDriver.idTemporalQuery(predicate);
            System.out.println("result count: " + resultCount);
            resultCountList.add(resultCount);
            blackhole.consume(predicate);
        }

        System.out.println("average result count: " + StatisticUtil.calculateAverage(resultCountList));
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SpringbokIdTemporalBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }

}
