package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;

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
      DataSet<YellowTaxyData> data = IOManager.generateDataSetFromParams(env, params);

      DataSet<YellowTaxyData> counts =
          // split up the lines in pairs (2-tuples) containing: (word,1)
          data.filter(new DataSetFilter()).distinct();

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
}
