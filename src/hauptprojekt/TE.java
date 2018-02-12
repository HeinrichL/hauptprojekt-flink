package hauptprojekt;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.TriangleEnumerator;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

public class TE {

	public static void main(String[] args) throws Exception {
		String file = args[0];

		ExecutionEnvironment env = Config.getEnv();

		Graph<LongValue, NullValue, NullValue> graph = Graph.fromCsvReader(Config.HDFS_URL + file, env).fieldDelimiterEdges(" ")
				.keyType(LongValue.class);

		// each tuple is a triangle
		DataSet<Tuple3<LongValue, LongValue, LongValue>> calc = graph.run(new TriangleEnumerator<LongValue, NullValue, NullValue>());
		System.out.println(calc.count());

		env.execute();
	}

}
