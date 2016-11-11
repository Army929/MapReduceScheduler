import java.util.ArrayList;
import java.util.List;

/**
 * 类说明
 *
 * @author:Amy
 * @version:2016年8月18日上午11:17:40
 */
public class Job {
	// public int id;
	public int mapTaskNum;

	public int reduceTaskNum;
	List<MapTask> mapTask = new ArrayList<MapTask>();
	List<ReduceTask> reduceTask = new ArrayList<ReduceTask>();

	//1、所有map的时间，为计算 map持续时间评估，为作业排序 w*T_low+(1-w)*T_up === 
	//2、重新赋值，作为map阶段该作业完成时间，sigma_m,为了Reduce前的排序
	public Double mapStageTime;
	
	//1、所有reduce的时间   为计算reduce持续时间评估 ，为作业排序 === 
	//2、重新赋值，作为reduce阶段该作业完成时间，sigma_r,只为记录作业当前运行的时间
	public Double reduceStageTime;
	
	
	//为了计算LB，引入计算每个job的map和reduce总的修正处理时间L(i,j)=P(i,j)+min S(i,j)
	//会对修正时间排序等操作，得到LB1,LB2
	public Double L_map_sumTime;
	public Double L_reduce_sumTime;
	
	 //TBS算法，为了让任务属于具体哪个作业明确，作业要有一个id即任务所属的作业
	public Integer jobId;
	
	public Integer getJobId() {
		return jobId;
	}
	public void setJobId(Integer jobId) {
		this.jobId = jobId;
	}

	public Double getReduceStageTime() {
		return reduceStageTime;
	}

	public void setReduceStageTime(Double reduceStageTime) {
		this.reduceStageTime = reduceStageTime;
	}

	public Double getMapStageTime() {
		return mapStageTime;
	}

	public void setMapStageTime(Double mapStageTime) {
		this.mapStageTime = mapStageTime;
	}

	public int getMapTaskNum() {
		return mapTaskNum;
	}

	public void setMapTaskNum(int mapTaskNum) {
		this.mapTaskNum = mapTaskNum;
	}

	public int getReduceTaskNum() {
		return reduceTaskNum;
	}

	public void setReduceTaskNum(int reduceTaskNum) {
		this.reduceTaskNum = reduceTaskNum;
	}

	public List<MapTask> getMapTask() {
		return mapTask;
	}

	public void setMapTask(List<MapTask> mapTask) {
		this.mapTask = mapTask;
	}

	public List<ReduceTask> getReduceTask() {
		return reduceTask;
	}

	public void setReduceTask(List<ReduceTask> reduceTask) {
		this.reduceTask = reduceTask;
	}
	
	public Double getL_map_sumTime() {
		return L_map_sumTime;
	}

	public void setL_map_sumTime(Double l_map_sumTime) {
		L_map_sumTime = l_map_sumTime;
	}

	public Double getL_reduce_sumTime() {
		return L_reduce_sumTime;
	}

	public void setL_reduce_sumTime(Double l_reduce_sumTime) {
		L_reduce_sumTime = l_reduce_sumTime;
	}

}
