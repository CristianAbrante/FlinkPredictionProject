package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.utils.ParameterTool;

public class IOManager {
  // String specifing option.
  public static String INPUT_OPTION = "input";

  /**
   * Method using for reading the dataset from the file, using the params of the program
   *
   * @param env flink execution environment.
   * @param params params of the program.
   * @return the dataframe
   * @throws IllegalArgumentException if option not specified.
   */
  public static DataSet<
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
      generateDataSetFromParams(ExecutionEnvironment env, ParameterTool params)
          throws IllegalArgumentException {
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
        data;
    if (params.has(IOManager.INPUT_OPTION)) {
      data =
          env.readCsvFile(params.getRequired(IOManager.INPUT_OPTION))
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
      throw new IllegalArgumentException(
          "Error: You have to specify an input file with the dataset, using --input option.");
    }
    return data;
  }
}
