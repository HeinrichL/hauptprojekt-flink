package strongly_connected_components;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.types.LongValue;

@SuppressWarnings("serial")
public class SCCBackPropSum extends SumFunction<SCCVertexValue, Tuple2<LongValue, LongValue>, Tuple2<Long, Long>> {

	// TODO: implement function
	@Override
	public Tuple2<Long, Long> sum(Tuple2<Long, Long> arg0, Tuple2<Long, Long> arg1) {
		throw new NotImplementedException();
	}
}
