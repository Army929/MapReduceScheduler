import java.util.ArrayList;
import java.util.List;

/**
 * ��˵��
 *
 * @author:Amy
 * @version:2016��8��18������11:17:40
 */
public class Job {
	// public int id;
	public int mapTaskNum;

	public int reduceTaskNum;
	List<MapTask> mapTask = new ArrayList<MapTask>();
	List<ReduceTask> reduceTask = new ArrayList<ReduceTask>();

	//1������map��ʱ�䣬Ϊ���� map����ʱ��������Ϊ��ҵ���� w*T_low+(1-w)*T_up === 
	//2�����¸�ֵ����Ϊmap�׶θ���ҵ���ʱ�䣬sigma_m,Ϊ��Reduceǰ������
	public Integer mapStageTime;
	
	//1������reduce��ʱ��   Ϊ����reduce����ʱ������ ��Ϊ��ҵ���� === 
	//2�����¸�ֵ����Ϊreduce�׶θ���ҵ���ʱ�䣬sigma_r,ֻΪ��¼��ҵ��ǰ���е�ʱ��
	public Integer reduceStageTime;
	
	
	//Ϊ�˼���LB���������ÿ��job��map��reduce�ܵ���������ʱ��L(i,j)=P(i,j)+min S(i,j)
	//�������ʱ������Ȳ������õ�LB1,LB2
	public Integer L_map_sumTime;
	public Integer L_reduce_sumTime;
	
	 //TBS�㷨��Ϊ�����������ھ����ĸ���ҵ��ȷ����ҵҪ��һ��id��������������ҵ
	public Integer jobId;
	
	public Integer getJobId() {
		return jobId;
	}
	public void setJobId(Integer jobId) {
		this.jobId = jobId;
	}

	public Integer getReduceStageTime() {
		return reduceStageTime;
	}

	public void setReduceStageTime(Integer reduceStageTime) {
		this.reduceStageTime = reduceStageTime;
	}

	public Integer getMapStageTime() {
		return mapStageTime;
	}

	public void setMapStageTime(Integer mapStageTime) {
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
	
	public Integer getL_map_sumTime() {
		return L_map_sumTime;
	}

	public void setL_map_sumTime(Integer l_map_sumTime) {
		L_map_sumTime = l_map_sumTime;
	}

	public Integer getL_reduce_sumTime() {
		return L_reduce_sumTime;
	}

	public void setL_reduce_sumTime(Integer l_reduce_sumTime) {
		L_reduce_sumTime = l_reduce_sumTime;
	}

}
