package scc;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.types.LongValue;

@SuppressWarnings("serial")
public class SCCForwardPropagationGather extends GatherFunction<LongValue, SCCVertexValue, Long> {

	@Override
	public void updateVertex(Vertex<LongValue, SCCVertexValue> vertex, MessageIterator<Long> inMessages)
			throws Exception {

		// get lowest id (color) from all active vertices
		SCCVertexValue vertexValue = vertex.f1;
		if (vertexValue.isActive()) {
			long minColor = Long.MAX_VALUE;

			for (Long l : inMessages) {
				if (l < minColor) {
					minColor = l;
				}
			}

			if (vertexValue.getColor() > minColor) {
				vertexValue.setColor(minColor);
				setNewVertexValue(vertexValue);
			}
		}
	}
}
