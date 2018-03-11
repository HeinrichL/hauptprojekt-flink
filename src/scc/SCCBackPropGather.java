package scc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.types.LongValue;

@SuppressWarnings("serial")
public class SCCBackPropGather
		extends GatherFunction<SCCVertexValue, Tuple2<LongValue, LongValue>, Boolean> {
	
	// TODO: implement function
	@Override
	public Boolean gather(Neighbor<SCCVertexValue, Tuple2<LongValue, LongValue>> neighbor) {
		// neighbor is root or root is reachable from neighbor
        SCCVertexValue neighborVertex = neighbor.f0;
        if(neighborVertex.isFinal() && (neighborVertex.isColorRoot() || neighborVertex.isColorRootReachable())){
            return true;
		}
		return false;
	}
}
