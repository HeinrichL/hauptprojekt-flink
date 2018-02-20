package hauptprojekt;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient;
import org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient.Result;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

public class LCdir {

	public static void main(String[] args) throws Exception {
		String file = args[0];

		ExecutionEnvironment env = Config.getEnv();

		Graph<LongValue, NullValue, NullValue> graph = Graph.fromCsvReader(Config.HDFS_URL + file, env).fieldDelimiterEdges(" ")
				.keyType(LongValue.class);

		DataSet<Result<LongValue>> r = graph.run(new LocalClusteringCoefficient<>());

		System.out.println(r.count());

		env.execute();
	}

}
