package strongly_connected_components;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.types.LongValue;


@SuppressWarnings("serial")
public class SCCBackPropApply extends ApplyFunction<LongValue, SCCVertexValue, Tuple2<Long, Long>> {

	// TODO: implement function
	@Override
	public void apply(Tuple2<Long, Long> newValue, SCCVertexValue currentValue) {
		throw new NotImplementedException();
	}
}
