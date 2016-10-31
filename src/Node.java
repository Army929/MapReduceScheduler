import java.util.List;

/**
 * 类说明：顶点类-表示任务
 *
 * @author:Amy
 * @version:2016年10月22日下午1:05:06
 */
public class Node {
	//与节点相连的两条边preEdge，nextEdge
	public List<Edge> preEdges;
	public List<Edge> nextEdges;
	//点的任务信息，runTime
	double nodeWeight;
	
	//rankU计算用于排序
	double rankU=0.0;
	
	//task type任务类型:1,2,3,4,5,6,7  按照拓扑从下到上node所在层 
	int taskType;
	
	//任务大小 size   为了分配任务 就节点3、5用到
	int taskSize;
	
	//任务当前完成时间 ，shuffle节点会使用到
	double stageTime;

	public List<Edge> getPreEdges() {
		return preEdges;
	}

	public void setPreEdges(List<Edge> preEdges) {
		this.preEdges = preEdges;
	}

	public List<Edge> getNextEdges() {
		return nextEdges;
	}

	public void setNextEdges(List<Edge> nextEdges) {
		this.nextEdges = nextEdges;
	}

	public double getNodeWeight() {
		return nodeWeight;
	}

	public void setNodeWeight(double nodeWeight) {
		this.nodeWeight = nodeWeight;
	}

	public double getRankU() {
		return rankU;
	}

	public void setRankU(double rankU) {
		this.rankU = rankU;
	}

	public int getTaskType() {
		return taskType;
	}

	public void setTaskType(int taskType) {
		this.taskType = taskType;
	}

	public int getTaskSize() {
		return taskSize;
	}

	public void setTaskSize(int taskSize) {
		this.taskSize = taskSize;
	}

	public double getStageTime() {
		return stageTime;
	}

	public void setStageTime(double stageTime) {
		this.stageTime = stageTime;
	}
	
//	MapTask nodeMapTask ;//=new MapTask();
//	ReduceTask nodeReduceTask;// = new ReduceTask();
//	
//	
//	//任务类型：map:0、reduce:1
//	public int taskType;
	
	 	
	
//	//任务运行时间
//	public int taskRuntime;
//	//任务文件大小
//	public int taskSize;
//	//任务类型：map、reduce
//	public int taskType;
//	
//	public int getTaskRuntime() {
//		return taskRuntime;
//	}
//	public void setTaskRuntime(int taskRuntime) {
//		this.taskRuntime = taskRuntime;
//	}
//	public int getTaskSize() {
//		return taskSize;
//	}
//	public void setTaskSize(int taskSize) {
//		this.taskSize = taskSize;
//	}
//	public int getTaskType() {
//		return taskType;
//	}
//	public void setTaskType(int taskType) {
//		this.taskType = taskType;
//	}

}
