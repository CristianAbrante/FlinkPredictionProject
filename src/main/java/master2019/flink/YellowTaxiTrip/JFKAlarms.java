package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableIterator;
import scala.Int;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

/** In this class the JFK airport trips program has to be implemented. */
public class JFKAlarms {
  public static void main(String[] args) throws Exception {
    // We construct the parameter object from args.
    final ParameterTool params = ParameterTool.fromArgs(args);

    // Load the execution environment. We do not need StreamExecutionEnvironment
    // as we are using static files.
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setParallelism(1);

    try {

      DataStream<Tuple4<Integer, String, String, Integer>> counts =
          IOManager.generateDataSetFromParams(env, params)
              .filter(new DataSetFilter())
              .assignTimestampsAndWatermarks(
                  new AscendingTimestampExtractor<YellowTaxyData>() {
                    @Override
                    public long extractAscendingTimestamp(YellowTaxyData yellowTaxyData) {
                      String pattern = "yyyy-MM-dd HH:mm:ss"; // ex: 2019-06-01 00:55:13
                      SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                      Date datePU = null;
                      try {
                        datePU = simpleDateFormat.parse(yellowTaxyData.getTpeppickupdatetime());
                      } catch (ParseException e) {
                        e.printStackTrace();
                      }
                      return datePU.getTime();
                    }
                  })
              .keyBy("vendorid")
              .window(TumblingEventTimeWindows.of(Time.hours(1)))
              .apply(new JFKApply());

      if (params.has(IOManager.OUTPUT_OPTION)) {
        counts.writeAsCsv(
            params.getRequired(IOManager.OUTPUT_OPTION), FileSystem.WriteMode.OVERWRITE);
        env.execute();
      } else {
        counts.print();
      }
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
    }
  }

  public static final class JFKApply
      implements WindowFunction<
          YellowTaxyData, Tuple4<Integer, String, String, Integer>, Tuple, TimeWindow> {
    public static Integer DEFAULT_VENDOR_ID = -1;
    public static String DEFAULT_PICKUP_DATE = "";
    public static String DEFAULT_DROPOFF_DATE = "";
    public static Integer DEFAULT_PASSENGERS_COUNT = 0;
    @Override
    public void apply(
        Tuple tuple,
        TimeWindow timeWindow,
        Iterable<YellowTaxyData> iterable,
        Collector<Tuple4<Integer, String, String, Integer>> collector)
        throws Exception {
      Iterator<YellowTaxyData> iterator = iterable.iterator();

      Integer vendorId = DEFAULT_VENDOR_ID;
      String pickupDate = DEFAULT_PICKUP_DATE;
      String dropoffDate = DEFAULT_DROPOFF_DATE;
      Integer passengersCount = DEFAULT_PASSENGERS_COUNT;

      while (iterator.hasNext()) {
        YellowTaxyData data = iterator.next();

        if (vendorId == DEFAULT_VENDOR_ID) {
          vendorId = data.getVendorid();
        }

        if (pickupDate.equals(DEFAULT_PICKUP_DATE)) {
          pickupDate = data.getTpeppickupdatetime();
        }

        if (!iterator.hasNext()) {
          dropoffDate = data.getTpepdropoffdatetime();
        }

        passengersCount += data.getPassengercount();
      }

      collector.collect(
              new Tuple4<>(vendorId, pickupDate, dropoffDate, passengersCount));
    }
  }

  public static final class DataSetFilter implements FilterFunction<YellowTaxyData> {
    public static int JFK_CODE = 2;
    public static int MIN_NUMBER_OF_PASSENGERS = 2;

    @Override
    public boolean filter(YellowTaxyData data) {
      return data.getRatecodeid() == DataSetFilter.JFK_CODE
          && data.getPassengercount() >= DataSetFilter.MIN_NUMBER_OF_PASSENGERS;
    }
  }
}
