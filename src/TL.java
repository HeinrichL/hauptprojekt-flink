import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.clustering.undirected.TriangleListing;
import org.apache.flink.types.LongValue;

public class TL {

	public static void main(String[] args) {
		String file = args[0];

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		Graph graph = Graph.fromCsvReader(file, env)
				.fieldDelimiterEdges(" ").keyType(LongValue.class);
		
		try {
			// DataSet contains all triangles, one record contains the three vertex ids
			DataSet calc = (DataSet) graph.run(new TriangleListing<>());
			System.out.println(calc.count());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
