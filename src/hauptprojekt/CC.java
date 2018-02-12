package hauptprojekt;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.ConnectedComponents;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

public class CC {

	public static void main(String[] args) throws Exception {
		String file = args[0];

		ExecutionEnvironment env = Config.getEnv();
		
		Graph graph = Graph.fromCsvReader(Config.HDFS_URL + file, env).fieldDelimiterEdges(" ").keyType(LongValue.class)
				.mapVertices(new MapFunction<Vertex<LongValue, NullValue>, LongValue>() {
					@Override
					public LongValue map(Vertex<LongValue, NullValue> arg0) throws Exception {
						// TODO Auto-generated method stub
						return arg0.getId();
					}
				});

		DataSet calc = (DataSet) graph.run(new ConnectedComponents<>(100));
		System.out.println(calc.count());

		env.execute();
	}

}
