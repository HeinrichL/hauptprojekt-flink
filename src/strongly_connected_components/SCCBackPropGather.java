package strongly_connected_components;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.types.LongValue;

@SuppressWarnings("serial")
public class SCCBackPropGather
		extends GatherFunction<SCCVertexValue, Tuple2<LongValue, LongValue>, Tuple2<Long, Long>> {
	
	// TODO: implement function
	@Override
	public Tuple2<Long, Long> gather(Neighbor<SCCVertexValue, Tuple2<LongValue, LongValue>> neighbor) {
		throw new NotImplementedException();
	}
}
