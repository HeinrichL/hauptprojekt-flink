package hauptprojekt;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAnalytic;
import org.apache.flink.graph.library.clustering.undirected.TriadicCensus.Result;
import org.apache.flink.graph.library.clustering.undirected.TriadicCensus;
import org.apache.flink.types.LongValue;

public class TC {

	public static void main(String[] args) throws Exception {
		String file = args[0];

		ExecutionEnvironment env = Config.getEnv();

		Graph graph = Graph.fromCsvReader(Config.HDFS_URL + file, env).fieldDelimiterEdges(" ")
				.keyType(LongValue.class);

		GraphAnalytic a;
		a = graph.run(new TriadicCensus<>());
		a.execute();

		Result r = (Result) a.getResult();

		System.out.println(r.getCount30());

		env.execute();
	}

}
