package strongly_connected_components;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.types.LongValue;

@SuppressWarnings("serial")
public class SCCBackPropSum extends SumFunction<SCCVertexValue, Tuple2<LongValue, LongValue>, Boolean> {

	// TODO: implement function
	@Override
	public Boolean sum(Boolean arg0, Boolean arg1) {
		return arg0 || arg1;
	}
}
