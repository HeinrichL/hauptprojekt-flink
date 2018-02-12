package hauptprojekt;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.clustering.undirected.TriangleListing;
import org.apache.flink.graph.library.clustering.undirected.TriangleListing.Result;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

public class TL {

	public static void main(String[] args) throws Exception {
		String file = args[0];

		ExecutionEnvironment env = Config.getEnv();

		Graph<LongValue, NullValue, NullValue> graph = Graph.fromCsvReader(Config.HDFS_URL + file, env).fieldDelimiterEdges(" ")
				.keyType(LongValue.class);

		// DataSet contains all triangles, one record contains the three vertex ids
		DataSet<Result<LongValue>> calc = graph.run(new TriangleListing<>());
		System.out.println(calc.count());

		env.execute();
	}

}
