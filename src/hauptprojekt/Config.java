package hauptprojekt;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;

public class Config {
	public static final String HDFS_URL = "hdfs://hdfs-namenode-0.hdfs-namenode.abk609.svc.cluster.local/";
	
	public static ExecutionEnvironment getEnv() {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, // number of restart attempts
				Time.of(10, TimeUnit.SECONDS) // delay
		));
		return env;
	}
}
