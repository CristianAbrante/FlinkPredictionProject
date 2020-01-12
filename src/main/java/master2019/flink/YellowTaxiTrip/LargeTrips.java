package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

/** In this class the Large trips program has to be implemented */
public class LargeTrips {

  // Reports Vendors (VendorID) that do >4 trips in a 3h period. Each one taking at least 20 minutes.
  // INPUT: Csv file with the information of the taxis
  // OUTPUT: Csv called "largeTips.csv" that contains the following variables:
  // "VendorID", "day", "numberOfTrips", "tpep_pickup_datetime" and "tpep_dropoff_datetime".
  public static void main(String[] args) throws Exception{

    // We construct the parameter object from args.
    final ParameterTool params = ParameterTool.fromArgs(args);

    // Load the execution environment.
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // input data and output too
    DataStream<String> inputFile;
    DataStream<String> outputFile;

    // read the text file from given input path
    inputFile = env.readTextFile(params.get("input"));
    //outputFile = env.readTextFile(params.get("output"));

    // make parameters available in the web interface
    env.getConfig().setGlobalJobParameters(params);
    //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

    SingleOutputStreamOperator<Tuple18<Integer,String,String,Double,Double,Double,String,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double>> mapStream = inputFile.
            map(new MapFunction<String, Tuple18<Integer,String,String,Double,Double,Double,String,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double>>() {
              public Tuple18<Integer,String,String,Double,Double,Double,String,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double> map(String in) throws Exception{
                String[] fieldArray = in.split(",");
                Tuple18<Integer,String,String,Double,Double,Double,String,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double> out = new Tuple18(
                        Integer.parseInt(fieldArray[0]),
                        fieldArray[1],
                        fieldArray[2],
                        Double.parseDouble(fieldArray[3]),
                        Double.parseDouble(fieldArray[4]),
                        Double.parseDouble(fieldArray[5]),
                        fieldArray[6],
                        Double.parseDouble(fieldArray[7]),
                        Double.parseDouble(fieldArray[8]),
                        Double.parseDouble(fieldArray[9]),
                        Double.parseDouble(fieldArray[10]),
                        Double.parseDouble(fieldArray[11]),
                        Double.parseDouble(fieldArray[12]),
                        Double.parseDouble(fieldArray[13]),
                        Double.parseDouble(fieldArray[14]),
                        Double.parseDouble(fieldArray[15]),
                        Double.parseDouble(fieldArray[16]),
                        Double.parseDouble(fieldArray[17])
                );
                Thread.sleep(500);
                return out;
              }
            });

    KeyedStream<Tuple18<Integer,String,String,Double,Double,Double,String,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double>, Tuple> keyedStream = mapStream
            .assignTimestampsAndWatermarks(new
               AscendingTimestampExtractor<Tuple18<Integer,String,String,Double,Double,Double,String,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double>>(){
                 @Override
                 public long extractAscendingTimestamp(Tuple18<Integer,String,String,Double,Double,Double,String,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double> element) {
                   String pattern = "yyyy-MM-dd HH:mm:ss"; //ex: 2019-06-01 00:55:13
                   SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                   Date datePU = null;
                   try {
                     datePU = simpleDateFormat.parse(element.f1);
                   } catch (ParseException e) {
                     e.printStackTrace();
                   }
                   return datePU.getTime();
                 }
               })
            .keyBy(0);

    SingleOutputStreamOperator<Tuple5<Integer,String,Integer,String,String>> timedTrips = keyedStream
            .window(TumblingProcessingTimeWindows.of(Time.hours(3)))
            .apply(new LargeTrips.TaxiTimerLargeTrips())
            .filter(new FilterFunction<Tuple5<Integer,String,Integer,String,String>>() {
              @Override
              public boolean filter(Tuple5<Integer,String,Integer,String,String> in) throws Exception {
                if(in.f2>4){
                  return true;
                } else {
                  return false;
                }
              }
            });

    // emit result
    if (params.has("output")) {
      timedTrips.writeAsCsv(params.get("output")+"largeTrips.csv");
    }

    env.setParallelism(1);

    // execute program
    env.execute("LargeTrips");
  }
  //Function that gets the data necessary from trips of more than 20m
  public static class TaxiTimerLargeTrips implements WindowFunction<
          Tuple18<Integer,String,String,Double,Double,Double,String,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double>,
          Tuple5<Integer,String,Integer,String,String>,
          Tuple,
          TimeWindow> {
    public void apply(Tuple tuple,
                      TimeWindow timeWindow,
                      Iterable<Tuple18<Integer,String,String,Double,Double,Double,String,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double>> input,
                      Collector<Tuple5<Integer,String,Integer,String,String>> out)
            throws Exception {
      Iterator<Tuple18<Integer,String,String,Double,Double,Double,String,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double>> iterator = input.iterator();

      Tuple18<Integer, // * VendorId
              String, // * tpep_pickup_datetime
              String, // * tpep_dropoff_datetime
              Double, // passenger_count
              Double, // trip_distance
              Double, // RatecodeID
              String, // store_and_fwd
              Double, // PUlocationID
              Double, // DOlocationID
              Double, // Payment_type
              Double, // fare_amount
              Double, // extra
              Double, // mta_tax
              Double, // tip_amount
              Double, // tolls_amount
              Double, // improvement_surcharge
              Double, // total_amount
              Double // congestion_surcharge
              > curr_tuple = iterator.next();

      Integer vid = 0;
      String PUd = "";
      String DOd = "";
      Date datePU = null;
      Date dateDO = null;
      String day = "";
      Integer trips = 0;

      String pattern = "yyyy-MM-dd HH:mm:ss"; //ex: 2019-06-01 00:55:13
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

      String pattern_day = "yyyy-MM-dd"; //ex: 2019-06-01
      SimpleDateFormat simpleDateFormat_day = new SimpleDateFormat(pattern_day);

      if(curr_tuple!=null){
        vid = curr_tuple.f0;  //Integer
        PUd = curr_tuple.f1;  //String
        DOd = curr_tuple.f2;  //String
        datePU = simpleDateFormat.parse(PUd);
        //dateDO = simpleDateFormat.parse(DOd);
        day = simpleDateFormat_day.format(datePU);
      }
      while(iterator.hasNext()){
        Tuple18<Integer,String,String,Double,Double,Double,String,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double> next = iterator.next();
        vid = next.f0;
        PUd = next.f1;
        DOd = next.f2;
        datePU = simpleDateFormat.parse(PUd);
        dateDO = simpleDateFormat.parse(DOd);
        long diffTime = datePU.getTime() - dateDO.getTime();
        if (diffTime > 1000 * 60 * 20 ){ //More than 20m trip
          trips += 1;
        }
      }
      out.collect(new Tuple5<Integer,String,Integer,String,String>
              (vid, day, trips, PUd, DOd));
    }
  }
}

