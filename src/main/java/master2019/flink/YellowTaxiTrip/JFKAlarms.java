package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableIterator;
import scala.Int;

import java.util.Iterator;

/** In this class the JFK airport trips program has to be implemented. */
public class JFKAlarms {
  public static void main(String[] args) throws Exception {
    // We construct the parameter object from args.
    final ParameterTool params = ParameterTool.fromArgs(args);

    // Load the execution environment. We do not need StreamExecutionEnvironment
    // as we are using static files.
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    env.setParallelism(1);

    try {
      DataSet<Tuple4<Integer, String, String, Integer>> counts =
          IOManager.generateDataSetFromParams(env, params)
              .filter(new DataSetFilter())
              .groupBy("vendorid", "pickupyear", "pickupmonth", "pickupday", "pickuphour")
              .reduceGroup(new JFKGroupReducer());

      counts.print();
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
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

  public static final class JFKGroupReducer
      implements GroupReduceFunction<YellowTaxyData, Tuple4<Integer, String, String, Integer>> {

    public static Integer DEFAULT_VENDOR_ID = -1;
    public static String DEFAULT_PICKUP_DATE = "";
    public static String DEFAULT_DROPOFF_DATE = "";
    public static Integer DEFAULT_PASSENGERS_COUNT = 0;

    @Override
    public void reduce(
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

      collector.collect(new Tuple4<>(vendorId, pickupDate, dropoffDate, passengersCount));
    }
  }
}
