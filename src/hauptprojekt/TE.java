package hauptprojekt;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.TriangleEnumerator;
import org.apache.flink.types.LongValue;

public class TE {

	public static void main(String[] args) throws Exception {
		String file = args[0];

		ExecutionEnvironment env = Config.getEnv();

		Graph graph = Graph.fromCsvReader(Config.HDFS_URL + file, env).fieldDelimiterEdges(" ")
				.keyType(LongValue.class);

		// each tuple is a triangle
		DataSet calc = (DataSet) graph.run(new TriangleEnumerator());
		System.out.println(calc.count());

		env.execute();
	}

}
