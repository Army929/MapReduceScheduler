import java.util.List;

/**
 * ��˵����������-��ʾ����
 *
 * @author:Amy
 * @version:2016��10��22������1:05:06
 */
public class Node {
	//��ڵ�������������preEdge��nextEdge
	public List<Edge> preEdges;
	public List<Edge> nextEdges;
	//���������Ϣ��runTime
	double nodeWeight;
	
	//rankU������������
	double rankU=0.0;
	
	//task type��������:1,2,3,4,5,6,7  �������˴��µ���node���ڲ� 
	int taskType;
	
	//�����С size   Ϊ�˷������� �ͽڵ�3��5�õ�
	int taskSize;
	
	//����ǰ���ʱ�� ��shuffle�ڵ��ʹ�õ�
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
//	//�������ͣ�map:0��reduce:1
//	public int taskType;
	
	 	
	
//	//��������ʱ��
//	public int taskRuntime;
//	//�����ļ���С
//	public int taskSize;
//	//�������ͣ�map��reduce
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
