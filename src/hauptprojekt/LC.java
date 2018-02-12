package hauptprojekt;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient.Result;
import org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient;
import org.apache.flink.types.LongValue;

public class LC {

	public static void main(String[] args) throws Exception {
		String file = args[0];

		ExecutionEnvironment env = Config.getEnv();

		Graph graph = Graph.fromCsvReader(Config.HDFS_URL + file, env).fieldDelimiterEdges(" ")
				.keyType(LongValue.class);

		DataSet<Result> r = (DataSet<Result>) graph.run(new LocalClusteringCoefficient<>());

		System.out.println(r.count());

		env.execute();
	}

}
