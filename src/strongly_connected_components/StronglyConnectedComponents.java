package strongly_connected_components;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.GSAConfiguration;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import hauptprojekt.Config;

public class StronglyConnectedComponents {
	public static void main(String[] args) throws Exception {
		String file = Config.HDFS_URL + args[0];
		int numIter = Integer.parseInt(args[1]); 

		ExecutionEnvironment env = Config.getEnv();

		Graph<LongValue, SCCVertexValue, Tuple2<LongValue, LongValue>> graph = Graph.fromCsvReader(file, env)
				.fieldDelimiterEdges(" ").keyType(LongValue.class).mapVertices(new VertexInitFunction())
				.mapEdges(new EdgeInitFunction());

		ScatterGatherConfiguration conf = new ScatterGatherConfiguration();
		conf.setDirection(EdgeDirection.ALL);

		int i = 0;
		while (i < numIter) {
			i += 1;
			// deactivate final vertices
			graph = graph.runScatterGatherIteration(new SCCFinalVerticesScatter(), new SCCFinalVerticesGather(), 5,
					conf);

			// propagate lowest ID
			conf.setDirection(EdgeDirection.OUT);
			graph = graph.runScatterGatherIteration(new SCCForwardPropagationScatter(),
					new SCCForwardPropagationGather(), 200, conf);

			// backpropagation of ID from last step
			GSAConfiguration gsaConf = new GSAConfiguration();
			gsaConf.setDirection(EdgeDirection.IN);
			//graph = graph.runGatherSumApplyIteration(new SCCBackPropGather(), new SCCBackPropSum(),
			//		new SCCBackPropApply(), 200, gsaConf);
		}
		graph.getVertices().print();
	}

	@SuppressWarnings("serial")
	private static final class EdgeInitFunction
			implements MapFunction<Edge<LongValue, NullValue>, Tuple2<LongValue, LongValue>> {
		@Override
		public Tuple2<LongValue, LongValue> map(Edge<LongValue, NullValue> arg0) throws Exception {
			Tuple2<LongValue, LongValue> t = new Tuple2<LongValue, LongValue>();
			t.f0 = arg0.getSource();
			t.f1 = arg0.getTarget();
			return t;
		}
	}

	@SuppressWarnings("serial")
	private static final class VertexInitFunction implements MapFunction<Vertex<LongValue, NullValue>, SCCVertexValue> {

		@Override
		public SCCVertexValue map(Vertex<LongValue, NullValue> arg0) throws Exception {
			return new SCCVertexValue(arg0.getId().getValue());
		}

	}
}
