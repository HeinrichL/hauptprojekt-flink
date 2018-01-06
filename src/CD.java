import java.util.Random;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.CommunityDetection;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

public class CD {

	public static void main(String[] args) {
		String file = args[0];

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Random r = new Random();
		
		Graph graph = Graph.fromCsvReader(Config.HDFS_URL + file, env)
				.fieldDelimiterEdges(" ").keyType(LongValue.class)
				.mapVertices(new MapFunction<Vertex<LongValue,NullValue>, Long>() {

					@Override
					public Long map(Vertex<LongValue, NullValue> arg0) throws Exception {
						// TODO Auto-generated method stub
						return r.nextLong() % (arg0.getId().getValue() + 1);
					}
					
				})
				.mapEdges(new MapFunction<Edge<LongValue,NullValue>, Double>() {

					@Override
					public Double map(Edge<LongValue, NullValue> arg0) throws Exception {
						// TODO Auto-generated method stub
						return 1.0;
					}
					
				});
		
		try {
			Graph calc = (Graph) graph.run(new CommunityDetection<LongValue>(5,0.5));
			System.out.println(calc.getVertices().collect());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
