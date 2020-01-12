package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

public class IOManager {
  // String specifing option.
  public static String INPUT_OPTION = "input";
  public static String OUTPUT_OPTION = "output";

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
                  "congestionamount")
              .map(new YellowTaxiCreator());
      return data;
    } else {
      throw new IllegalArgumentException(
          "Error: You have to specify an input file with the dataset, using --input option.");
    }
  }

  public static final class YellowTaxiCreator
      implements MapFunction<YellowTaxyData, YellowTaxyData> {
    @Override
    public YellowTaxyData map(YellowTaxyData yellowTaxyData) throws Exception {
      return new YellowTaxyData(
          yellowTaxyData.getVendorid(),
          yellowTaxyData.getTpeppickupdatetime(),
          yellowTaxyData.getTpepdropoffdatetime(),
          yellowTaxyData.getPassengercount(),
          yellowTaxyData.getTripdistance(),
          yellowTaxyData.getRatecodeid(),
          yellowTaxyData.getStoreandfwdflag(),
          yellowTaxyData.getPulocationid(),
          yellowTaxyData.getDolocationid(),
          yellowTaxyData.getPaymenttype(),
          yellowTaxyData.getFareamount(),
          yellowTaxyData.getExtra(),
          yellowTaxyData.getMtatax(),
          yellowTaxyData.getTipamount(),
          yellowTaxyData.getTollsamount(),
          yellowTaxyData.getImprovementsurcharge(),
          yellowTaxyData.getTotalamount(),
          yellowTaxyData.getCongestionamount());
    }
  }
}
