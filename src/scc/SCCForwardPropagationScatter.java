package scc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.types.LongValue;

@SuppressWarnings("serial")
public class SCCForwardPropagationScatter extends ScatterFunction<LongValue, SCCVertexValue, Long, Tuple2<LongValue, LongValue>> {

	@Override
	public void sendMessages(Vertex<LongValue, SCCVertexValue> vertex) throws Exception {
		
		// Propagate minimum color through all outgoing edges
		SCCVertexValue vertexValue = vertex.f1;
		if(vertexValue.isActive())
			sendMessageToAllNeighbors(Math.min(vertexValue.getColor(), vertexValue.getId()));
	}

}
