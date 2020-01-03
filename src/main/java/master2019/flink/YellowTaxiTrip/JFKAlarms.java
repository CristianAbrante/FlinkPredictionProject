package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.Date;

/** In this class the JFK airport trips program has to be implemented. */
public class JFKAlarms {
  public static void main(String[] args) throws Exception {

    // We construct the parameter object from args.
    final ParameterTool params = ParameterTool.fromArgs(args);

    // Load the execution environment. We do not need StreamExecutionEnvironment
    // as we are using static files.
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // This stream is going to contain the dataset.
    DataSet<
            Tuple18<
                Integer, // VendorId
                String, // tpep_pickup_datetime
                String,
                Double,
                Double,
                Double,
                String,
                Double,
                Double,
                Double,
                Double,
                Double,
                Double,
                Double,
                Double,
                Double,
                Double,
                Double>>
        data = null;

    if (params.has("input")) {
      data =
          env.readCsvFile(params.getRequired("input"))
              .types(
                  Integer.class,
                  String.class,
                  String.class,
                  Double.class,
                  Double.class,
                  Double.class,
                  String.class,
                  Double.class,
                  Double.class,
                  Double.class,
                  Double.class,
                  Double.class,
                  Double.class,
                  Double.class,
                  Double.class,
                  Double.class,
                  Double.class,
                  Double.class);
    } else {
      System.out.println("Error: You have to specify an input file with the dataset.");
      System.out.println("Help: use --input option");
      return;
    }

    DataSet<Tuple2<String, Integer>> counts =
        // split up the lines in pairs (2-tuples) containing: (word,1)
        data.flatMap(new Tokenizer());

    counts.print();
    env.execute("JFK Alarms streaming");
  }

  public static final class Tokenizer
      implements FlatMapFunction<
          Tuple18<
              Integer, // VendorId
              String, // tpep_pickup_datetime
              String,
              Double,
              Double,
              Double,
              String,
              Double,
              Double,
              Double,
              Double,
              Double,
              Double,
              Double,
              Double,
              Double,
              Double,
              Double>,
          Tuple2<String, Integer>> {

    @Override
    public void flatMap(
        Tuple18<
                Integer, // VendorId
                String, // tpep_pickup_datetime
                String,
                Double,
                Double,
                Double,
                String,
                Double,
                Double,
                Double,
                Double,
                Double,
                Double,
                Double,
                Double,
                Double,
                Double,
                Double>
            value,
        Collector<Tuple2<String, Integer>> out) {

      System.out.println(value.f0 + " " + value.f1 + " " + value.f2);
      // normalize and split the line
      out.collect(new Tuple2<String, Integer>("", 1));
    }
  }
}
