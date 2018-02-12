package hauptprojekt;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAnalytic;
import org.apache.flink.graph.library.clustering.directed.AverageClusteringCoefficient;
import org.apache.flink.graph.library.clustering.directed.AverageClusteringCoefficient.Result;
import org.apache.flink.types.LongValue;

public class AC {

	public static void main(String[] args) throws Exception {
		String file = args[0];

		ExecutionEnvironment env = Config.getEnv();

		Graph graph = Graph.fromCsvReader(Config.HDFS_URL + file, env).fieldDelimiterEdges(" ")
				.keyType(LongValue.class);

		GraphAnalytic a;
		a = graph.run(new AverageClusteringCoefficient<>());
		a.execute();

		Result r = (Result) a.getResult();

		System.out.println(r.getAverageClusteringCoefficient());
		
		env.execute();
	}

}
