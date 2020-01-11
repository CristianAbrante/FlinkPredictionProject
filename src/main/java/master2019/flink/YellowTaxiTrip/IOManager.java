package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
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
  public static DataSet<YellowTaxyData> generateDataSetFromParams(
      ExecutionEnvironment env, ParameterTool params) throws IllegalArgumentException {
    DataSet<YellowTaxyData> data;
    if (params.has(IOManager.INPUT_OPTION)) {
      data =
          env.readCsvFile(params.getRequired(IOManager.INPUT_OPTION))
              .pojoType(
                  YellowTaxyData.class,
                  "vendorid",
                  "tpeppickupdatetime",
                  "tpepdropoffdatetime",
                  "passengercount",
                  "tripdistance",
                  "ratecodeid",
                  "storeandfwdflag",
                  "pulocationid",
                  "dolocationid",
                  "paymenttype",
                  "fareamount",
                  "extra",
                  "mtatax",
                  "tipamount",
                  "tollsamount",
                  "improvementsurcharge",
                  "totalamount",
                  "congestionamount");
      return data;
    } else {
      throw new IllegalArgumentException(
          "Error: You have to specify an input file with the dataset, using --input option.");
    }
  }
}
