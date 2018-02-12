package hauptprojekt;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAnalytic;
import org.apache.flink.graph.library.clustering.directed.GlobalClusteringCoefficient;
import org.apache.flink.graph.library.clustering.directed.GlobalClusteringCoefficient.Result;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

public class GC {

	public static void main(String[] args) throws Exception {
		String file = args[0];

		ExecutionEnvironment env = Config.getEnv();

		Graph<LongValue, NullValue, NullValue> graph = Graph.fromCsvReader(Config.HDFS_URL + file, env).fieldDelimiterEdges(" ")
				.keyType(LongValue.class);

		GraphAnalytic<LongValue, NullValue, NullValue, GlobalClusteringCoefficient.Result> a;
		a = graph.run(new GlobalClusteringCoefficient<LongValue, NullValue, NullValue>());
		a.execute();

		Result r = a.getResult();

		System.out.println(r.getGlobalClusteringCoefficientScore());

		env.execute();
	}

}
