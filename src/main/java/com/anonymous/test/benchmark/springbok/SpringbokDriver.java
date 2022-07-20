package com.anonymous.test.benchmark.springbok;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.anonymous.test.benchmark.predicate.QueryGenerator;
import com.anonymous.test.client.SpringbokClient;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.predicate.IdTemporalQueryPredicate;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;

import java.util.*;

/**
 * @author anonymous
 * @create 2022-06-28 11:35 AM
 **/
public class SpringbokDriver {

    private static SpringbokClient client = new SpringbokClient("http://localhost:8001");

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        /*List<IdTemporalQueryPredicate> predicateList = QueryGenerator.getIdTemporalQueriesFromQueryFile("/home/anonymous/Data/DataSet/Trajectory/TaxiPorto/archive/query-on-10w/porto_10w_id_7d.query");
        long start = System.currentTimeMillis();
        for (IdTemporalQueryPredicate predicate : predicateList) {
            int count = idTemporalQuery(predicate);
            System.out.println("count: " + count);
        }
        long stop = System.currentTimeMillis();
        System.out.println("50 queries time: " + (stop - start) + " ms");*/

        /*IdTemporalQueryPredicate predicate = new IdTemporalQueryPredicate(1372638215000L,1373243015000L,"20000337");
        List<TrajectoryPoint> pointList = idTemporalQueryForCheck(predicate);
        Set<Long> hashSet = new HashSet<>();
        for (TrajectoryPoint point : pointList) {
            hashSet.add(point.getTimestamp());
        }
        System.out.println(hashSet.size());*/

        List<SpatialTemporalRangeQueryPredicate> predicateList = QueryGenerator.getSpatialTemporalRangeQueriesFromQueryFile("/home/anonymous/Data/DataSet/Trajectory/TaxiPorto/archive/query-on-10w/porto_10w_24h_01.query");
        long start = System.currentTimeMillis();
        for (SpatialTemporalRangeQueryPredicate predicate : predicateList) {
            System.out.println(predicate);
            int count = spatialTemporalQuery(predicate);
            System.out.println("count: " + count);
        }
        long stop = System.currentTimeMillis();
        System.out.println("50 queries time: " + (stop - start) + " ms");

    }

    public static void insertData(List<TrajectoryPoint> trajectoryPointList) {
        try {
            client.insert(trajectoryPointList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void insertDataAsync(List<TrajectoryPoint> trajectoryPointList) {
        try {
            client.insert(trajectoryPointList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int idTemporalQuery(IdTemporalQueryPredicate predicate) {
        try {
            String result = client.idTemporalQuery(predicate);
            List<TrajectoryPoint> pointList = objectMapper.readValue(result, new TypeReference<List<TrajectoryPoint>>() {});

            return pointList.size();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static List<TrajectoryPoint> idTemporalQueryForCheck(IdTemporalQueryPredicate predicate) {
        try {
            String result = client.idTemporalQuery(predicate);
            List<TrajectoryPoint> pointList = objectMapper.readValue(result, new TypeReference<List<TrajectoryPoint>>() {});

            pointList.sort(Comparator.comparingLong(TrajectoryPoint::getTimestamp));
            for (TrajectoryPoint point : pointList) {
                System.out.println(point);
            }
            return pointList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static int spatialTemporalQuery(SpatialTemporalRangeQueryPredicate predicate) {
        try {
            String result = client.spatialTemporalQuery(predicate);
            List<TrajectoryPoint> pointList = objectMapper.readValue(result, new TypeReference<List<TrajectoryPoint>>() {});
            return pointList.size();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }



}
