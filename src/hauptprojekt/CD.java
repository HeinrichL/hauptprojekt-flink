package hauptprojekt;
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

	@SuppressWarnings("serial")
	private static final class CDEdgeInitFunction implements MapFunction<Edge<LongValue, NullValue>, Double> {
		@Override
		public Double map(Edge<LongValue, NullValue> arg0) throws Exception {
			// TODO Auto-generated method stub
			return 1.0;
		}
	}

	@SuppressWarnings("serial")
	private static final class CDVertexInitFunction implements MapFunction<Vertex<LongValue, NullValue>, Long> {
		private final Random r;

		private CDVertexInitFunction(Random r) {
			this.r = r;
		}

		@Override
		public Long map(Vertex<LongValue, NullValue> arg0) throws Exception {
			// TODO Auto-generated method stub
			return r.nextLong() % (arg0.getId().getValue() + 1);
		}
	}

	public static void main(String[] args) throws Exception {
		String file = args[0];
		ExecutionEnvironment env = Config.getEnv();
		Random r = new Random();

		Graph<LongValue, Long, Double> graph = Graph.fromCsvReader(Config.HDFS_URL + file, env).fieldDelimiterEdges(" ").keyType(LongValue.class)
				.mapVertices(new CDVertexInitFunction(r)).mapEdges(new CDEdgeInitFunction());

		Graph<LongValue, Long, Double> calc = graph.run(new CommunityDetection<LongValue>(5, 0.5));
		System.out.println(calc.getVertices().collect());

		env.execute();
	}

}
