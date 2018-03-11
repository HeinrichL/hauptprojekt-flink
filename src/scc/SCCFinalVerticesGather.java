package scc;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.types.LongValue;

@SuppressWarnings("serial")
public class SCCFinalVerticesGather extends GatherFunction<LongValue, SCCVertexValue, String> {

	@Override
	public void updateVertex(Vertex<LongValue, SCCVertexValue> vertex, MessageIterator<String> inMessages)
			throws Exception {
		long inDeg;
		long outDeg;
		SCCVertexValue vertexValue = vertex.f1;
		if (vertexValue.isFirstIteration()) {
			inDeg = 0;
			outDeg = 0;
			
			// Count in/out degrees and save them in vertex
			for (String s : inMessages) {
				if (s.equals("IN"))
					inDeg++;
				else if (s.equals("OUT"))
					outDeg++;
			}

			vertexValue.setInDegree(inDeg);
			vertexValue.setOutDegree(outDeg);
			vertexValue.setFirstIteration(false);

		} else {
			if (vertexValue.isActive()) {
				inDeg = vertexValue.getInDegree();
				outDeg = vertexValue.getOutDegree();
				
				// decrease in/out degrees based on received messages or deactivate myself when appropriate message received
				for (String s : inMessages) {
					if (s.equals("DEACTIVATE"))
						vertexValue.setActive(false);
					else if (s.equals("IN"))
						inDeg--;
					else if (s.equals("OUT"))
						outDeg--;
				}
				vertexValue.setInDegree(inDeg);
				vertexValue.setOutDegree(outDeg);
			}
		}
		
		// I am final when my in or out Degree is zero, because then I am my own SCC.
		if (vertexValue.vertexIsFinal()) {
			vertexValue.setFinal(true);
			vertexValue.setColor(vertex.getId().getValue());
		}
		setNewVertexValue(vertexValue);
	}

}
