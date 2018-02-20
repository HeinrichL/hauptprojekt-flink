package strongly_connected_components;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.types.LongValue;


@SuppressWarnings("serial")
public class SCCBackPropApply extends ApplyFunction<LongValue, SCCVertexValue, Boolean> {

	// TODO: implement function
	@Override
	public void apply(Boolean colorRootIsReachable, SCCVertexValue currentVertex) {

	    boolean isColorRoot = currentVertex.getId() == currentVertex.getColor();

        if(!currentVertex.isFinal() && (isColorRoot || colorRootIsReachable)) {
            currentVertex.setColorRoot(isColorRoot);
            currentVertex.setColorRootReachable(colorRootIsReachable);
            currentVertex.setFinal(true);
            currentVertex.setActive(false);
            setResult(currentVertex);
        }
	}
}
