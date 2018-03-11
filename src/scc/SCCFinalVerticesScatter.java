package scc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.types.LongValue;

@SuppressWarnings("serial")
public class SCCFinalVerticesScatter extends ScatterFunction<LongValue, SCCVertexValue, String, Tuple2<LongValue, LongValue>> {

	@Override
	public void sendMessages(Vertex<LongValue, SCCVertexValue> vertex) throws Exception {
		SCCVertexValue vertexValue = vertex.f1;
		if (vertexValue.isFirstIteration()) {
			// Inform neighbors that I am an in/out neighbor of them
			for (Edge<LongValue, Tuple2<LongValue, LongValue>> e : getEdges()) {
				if (e.getSource().getValue() == vertex.getId().getValue())
					// I am the source, so neighbor has an incoming edge
					sendMessageTo(e.getTarget(), "IN");
				else if (e.getTarget().getValue() == vertex.getId().getValue())
					// I am the target, so neighbor has an outgoing edge
					sendMessageTo(e.getSource(), "OUT");
			}
		} else {
			// If I am final then deactivate myself and inform neighbors so they can decrease their degree  
			if (vertexValue.isFinal() && vertexValue.isActive()) {
				for (Edge<LongValue, Tuple2<LongValue, LongValue>> e : getEdges()) {
					if (e.getSource().getValue() == vertex.getId().getValue())
						// I am the source, so neighbor has to decrease his in degree
						sendMessageTo(e.getTarget(), "IN");
					else if (e.getTarget().getValue() == vertex.getId().getValue())
						// I am the target, so neighbor has to decrease his out degree
						sendMessageTo(e.getSource(), "OUT");
				}
				// Send message to me to deactivate myself in next Gather phase
				sendMessageTo(vertex.getId(), "DEACTIVATE");
			}
		}
	}
}
