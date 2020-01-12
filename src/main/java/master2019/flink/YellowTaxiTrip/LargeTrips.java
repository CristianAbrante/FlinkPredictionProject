package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
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

    // read the text file from given input path
    inputFile = env.readTextFile(params.get("input"));

    // make parameters available in the web interface
    env.getConfig().setGlobalJobParameters(params);

    SingleOutputStreamOperator<Tuple3<Integer,String,String>> mapStream = inputFile.
            map(new MapFunction<String, Tuple3<Integer,String,String>>() {
              public Tuple3<Integer,String,String> map(String in) throws Exception{
                String[] fieldArray = in.split(",");
                Tuple3<Integer,String,String> out = new Tuple3(
                        Integer.parseInt(fieldArray[0]),
                        fieldArray[1],
                        fieldArray[2]
                );
                return out;
              }
            });

    KeyedStream<Tuple3<Integer,String,String>, Tuple> keyedStream = mapStream
            .filter(new FilterFunction<Tuple3<Integer,String,String>>() {
              @Override
              public boolean filter(Tuple3<Integer,String,String> in) throws Exception {
                String pattern = "yyyy-MM-dd HH:mm:ss"; //ex: 2019-06-01 00:55:13
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                Date datePU = null;
                Date dateDO = null;
                if ((in.f1 == "") || (in.f2 == "")){
                    return false;
                }
                try {
                  datePU = simpleDateFormat.parse(in.f1);
                  dateDO = simpleDateFormat.parse(in.f2);
                } catch (ParseException e) {
                  e.printStackTrace();
                  return false;
                }
                long diffTime = dateDO.getTime() - datePU.getTime();
                if (diffTime >= 1000 * 60 * 20 ){ //More than 20m trip
                  return true;
                }
                else { return false; }
              }
            })
            .assignTimestampsAndWatermarks(new
               AscendingTimestampExtractor<Tuple3<Integer,String,String>>(){
                 @Override
                 public long extractAscendingTimestamp(Tuple3<Integer,String,String> element) {
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
            .window(TumblingEventTimeWindows.of(Time.hours(3)))
            .apply(new LargeTrips.TaxiTimerLargeTrips())
;

    // emit result
    if (params.has("output")) {
      timedTrips.writeAsCsv(params.get("output"),  FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }

    // execute program
    env.execute("LargeTrips");
  }
  //Function that gets the data necessary from trips of more than 20m
  public static class TaxiTimerLargeTrips implements WindowFunction<
          Tuple3<Integer,String,String>,
          Tuple5<Integer,String,Integer,String,String>,
          Tuple,
          TimeWindow> {
    public void apply(Tuple tuple,
                      TimeWindow timeWindow,
                      Iterable<Tuple3<Integer,String,String>> input,
                      Collector<Tuple5<Integer,String,Integer,String,String>> out)
            throws Exception {
      Iterator<Tuple3<Integer,String,String>> iterator = input.iterator();

      Tuple3<Integer,String,String> curr_tuple = iterator.next();

      Integer vid = 0;
      String PUd = "";
      String fPUd = "";
      String DOd = "";
      Date datePU = null;
      Date dateDO = null;
      String day = "";
      Integer trips = 1;

      String pattern = "yyyy-MM-dd HH:mm:ss"; //ex: 2019-06-01 00:55:13
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

      String pattern_day = "yyyy-MM-dd"; //ex: 2019-06-01
      SimpleDateFormat simpleDateFormat_day = new SimpleDateFormat(pattern_day);

      if(curr_tuple!=null){
        vid = curr_tuple.f0;  //Integer
        PUd = curr_tuple.f1;  //String
        DOd = curr_tuple.f2;  //String
        fPUd = curr_tuple.f1;  //String
        datePU = simpleDateFormat.parse(PUd);
        day = fPUd.split(" ")[0];
      }
      while(iterator.hasNext()){
        Tuple3<Integer,String,String> next = iterator.next();
        //vid = next.f0;
        //PUd = next.f1;
        DOd = next.f2;
//        datePU = simpleDateFormat.parse(PUd);
//        dateDO = simpleDateFormat.parse(DOd);
//        long diffTime = datePU.getTime() - dateDO.getTime();
//        if (diffTime > 1000 * 60 * 20 ){ //More than 20m trip
          trips += 1;
        //}
      }
      out.collect(new Tuple5<Integer,String,Integer,String,String>
              (vid, day, trips, fPUd, DOd));
    }
  }
}

