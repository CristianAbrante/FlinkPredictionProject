package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
  public static DataStream<YellowTaxyData> generateDataSetFromParams(
      StreamExecutionEnvironment env, ParameterTool params) throws IllegalArgumentException {
    DataStream<YellowTaxyData> data;
    if (params.has(IOManager.INPUT_OPTION)) {
      data =
          env.readTextFile(params.getRequired(IOManager.INPUT_OPTION)).map(new YellowTaxiCreator());
      return data;
    } else {
      throw new IllegalArgumentException(
          "Error: You have to specify an input file with the dataset, using --input option.");
    }
  }

  public static DataSet<
          Tuple9<Integer, String, String, Integer, Integer, String, String, String, String>>
      getTestData(ExecutionEnvironment env, ParameterTool params) {
    DataSet<Tuple9<Integer, String, String, Integer, Integer, String, String, String, String>>
        data =
            env.readCsvFile(params.getRequired("input"))
                .types(
                    Integer.class,
                    String.class,
                    String.class,
                    Integer.class,
                    Integer.class,
                    Integer.class,
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
                    Double.class)
                .map(
                    new MapFunction<
                        Tuple18<
                            Integer,
                            String,
                            String,
                            Integer,
                            Integer,
                            Integer,
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
                        Tuple9<
                            Integer,
                            String,
                            String,
                            Integer,
                            Integer,
                            String,
                            String,
                            String,
                            String>>() {
                      @Override
                      public Tuple9<
                              Integer,
                              String,
                              String,
                              Integer,
                              Integer,
                              String,
                              String,
                              String,
                              String>
                          map(
                              Tuple18<
                                      Integer,
                                      String,
                                      String,
                                      Integer,
                                      Integer,
                                      Integer,
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
                                  data)
                              throws Exception {
                        String[] timestampSplit = data.f1.split(" ");
                        String datePart = timestampSplit[0];
                        String timePart = timestampSplit[1];

                        String[] datePartSplit = datePart.split("-");
                        String[] timePartSplit = timePart.split(":");
                        return new Tuple9<>(
                            data.f0,
                            data.f1,
                            data.f2,
                            data.f3,
                            data.f5,
                            datePartSplit[0],
                            datePartSplit[1],
                            datePartSplit[2],
                            timePartSplit[0]);
                      }
                    });
    return data;
  }

  public static final class YellowTaxiCreator implements MapFunction<String, YellowTaxyData> {
    @Override
    public YellowTaxyData map(String line) throws Exception {
      String[] splittedLine = line.split(",");
      return new YellowTaxyData(
          Integer.parseInt(splittedLine[0]),
          splittedLine[1],
          splittedLine[2],
          Integer.parseInt(splittedLine[3]),
          Double.parseDouble(splittedLine[4]),
          Integer.parseInt(splittedLine[5]),
          splittedLine[6],
          Integer.parseInt(splittedLine[7]),
          Integer.parseInt(splittedLine[8]),
          Integer.parseInt(splittedLine[9]),
          Double.parseDouble(splittedLine[10]),
          Double.parseDouble(splittedLine[11]),
          Double.parseDouble(splittedLine[12]),
          Double.parseDouble(splittedLine[13]),
          Double.parseDouble(splittedLine[14]),
          Double.parseDouble(splittedLine[15]),
          Double.parseDouble(splittedLine[16]),
          Double.parseDouble(splittedLine[17]));
    }
  }
}
