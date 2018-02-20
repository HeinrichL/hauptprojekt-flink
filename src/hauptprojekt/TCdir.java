package hauptprojekt;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAnalytic;
import org.apache.flink.graph.library.clustering.directed.TriadicCensus;
import org.apache.flink.graph.library.clustering.directed.TriadicCensus.Result;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

public class TCdir {

	public static void main(String[] args) throws Exception {
		String file = args[0];

		ExecutionEnvironment env = Config.getEnv();

		Graph<LongValue, NullValue, NullValue> graph = Graph.fromCsvReader(Config.HDFS_URL + file, env).fieldDelimiterEdges(" ")
				.keyType(LongValue.class);

		GraphAnalytic<LongValue, NullValue, NullValue, Result> a;
		a = graph.run(new TriadicCensus<>());
		a.execute();

		Result r = (Result) a.getResult();

		System.out.println(r.getCount003());

		env.execute();
	}

}
