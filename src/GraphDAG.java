import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * ��˵��
 *
 * @author:Amy
 * @version:2016��10��22������1:05:36
 */
public class GraphDAG {
	// ����ͷ�ڵ�head node
	// Node head=new Node();
	// ���е㼯��
	static List<Node> nodeList ;//= new ArrayList<Node>();
	// ���б߼���
	static List<Edge> edgeList;// = new ArrayList<Edge>();
	static int fd = 100;

	static int fr = 50;

	static int fn = 30;

	public static void initDAGData(List<Job> listJobs) {
		 nodeList = new ArrayList<Node>();
		 edgeList = new ArrayList<Edge>();
		// String path =
		// "D:\\JaveWeb\\MapReduceScheduler\\DataSet\\Job-50\\Job-50-0.txt";
		//
//		List<Double> shuffleList = new ArrayList<Double>();
//		for (Job jb : listJobs) {
//			double sumMap = 0;
//			for (MapTask mt : jb.mapTask) {
//				sumMap += mt.mapRunTime + (mt.mapSize / fd + mt.mapSize / fn + mt.mapSetupTime / fr) / 3;
//			}
//			shuffleList.add(sumMap);
//		}
//		sortShuffle(shuffleList);
//		for (double i : shuffleList) {
//			// System.out.println("job sum maptaskTime:" + i);
//		}
		// System.out.println("shuffleList size:"+shuffleList.size());

		// ��ʼ����һ����ʼ�ܵĽڵ�(����)->job�����ڵ�(����)->map �������ڵ�->
		// Shuffle�ڵ�(����)->reduce �������ڵ�->job��ҵ����(����)->һ���ܽ����ڵ�(����)
		// n1
		Node head = new Node();
		head.preEdges = new ArrayList<Edge>();// List<Edge>
		head.nextEdges = null;
		head.nodeWeight = 0.0;
		head.rankU = 0.0;
		head.taskType = 1; // ֻ��1��
		// head.taskSize = 0;

		// n7
		Node tail = new Node();
		tail.preEdges = null;// List<Edge>
		tail.nextEdges = new ArrayList<Edge>();// List<Edge>
		tail.nodeWeight = 0.0;
		tail.rankU = 0.0;
		tail.taskType = 7;// ֻ��1��
		// tail.taskSize = 0;
		// ��node�ڵ��б�
		nodeList.add(head);// �ڵ㼯����Ӹ�Ԫ��------------------------------------��� n1
		for (Job jb : listJobs) {
			// Ϊ����LB��ÿ����ҵ��map��reduce�ֱ��������ʱ��
			Double map_LSumTime = 0.0;
			Double reduce_LSumTime = 0.0;

			// n2
			Node nodeJobEnd = new Node();
			nodeJobEnd.preEdges = new ArrayList<Edge>();
			nodeJobEnd.nextEdges = new ArrayList<Edge>();// һ����
			nodeJobEnd.nodeWeight = 0.0;// ��ʼ������
			nodeJobEnd.rankU = 0.0;
			nodeJobEnd.taskType = 2;
			// nodeJobEnd.taskSize = 0;

			// e1
			Edge egJobEnd = new Edge();
			egJobEnd.preNode = nodeJobEnd;// ��һ��������ҵ node
			egJobEnd.nextNode = head;// ��һ����head
			egJobEnd.edgeWeight = 0;

			// n1 ���e1
			head.preEdges.add(egJobEnd);

			// һ��Ԫ�ص�List n2���e1
			List<Edge> tmpEdgeJobEnd = new ArrayList<Edge>();
			tmpEdgeJobEnd.add(egJobEnd);
			nodeJobEnd.nextEdges = tmpEdgeJobEnd;

			edgeList.add(egJobEnd);// �߼�����Ӹ�Ԫ��-----------------------------------���
									// e1
			nodeList.add(nodeJobEnd);// �ڵ㼯����Ӹ�Ԫ��--------------------------------���
										// n2

			// ����shufflt�ڵ㣬Ϊ��reduce ������map������Լ��ϵ���� n4
			Node nodeShuffle = new Node();
			nodeShuffle.preEdges = new ArrayList<Edge>();
			nodeShuffle.nextEdges = new ArrayList<Edge>();
			nodeShuffle.nodeWeight = 0.0; // ����ڵ�Ȩ��Ϊ0
			nodeShuffle.rankU = 0.0;
			nodeShuffle.taskType = 4;
			// nodeShuffle.taskSize=0;

			// �����һ��reduce��㼯��
			for (ReduceTask rt : jb.reduceTask) {
				reduce_LSumTime += (rt.reduceRunTime + rt.reduceSize / fd);// -----------------------�����ٶ�ѡ��
				// n3
				Node nodeReduce = new Node();
				nodeReduce.preEdges = new ArrayList<Edge>();// һ����
				nodeReduce.nextEdges = new ArrayList<Edge>();// һ����
				nodeReduce.nodeWeight = rt.reduceRunTime;// ��ʼ������
				nodeReduce.rankU = 0.0;
				nodeReduce.taskType = 3;
				nodeReduce.taskSize = rt.reduceSize;

				// e2
				Edge egReduceDown = new Edge();
				egReduceDown.preNode = nodeReduce;// ��һ���ڵ�����reduce ����
				egReduceDown.nextNode = nodeJobEnd;// ��һ����nodeJob
				egReduceDown.edgeWeight = 0.0;// �ñߴ�С����Ϊ0

				// �ڵ����һ���� n3���e2
				List<Edge> egReduceDownList = new ArrayList<Edge>();
				egReduceDownList.add(egReduceDown);
				nodeReduce.nextEdges = egReduceDownList;

				// ��ҵ�ڵ����ǰ���� ��һ�����ϣ��кܶ��� n2�ڵ�ǰ���ж����߼� n2���e2
				nodeJobEnd.preEdges.add(egReduceDown);

				// �㼯�Ϻͱ߼��Ϸֱ������Ԫ��
				edgeList.add(egReduceDown); // �߼�����Ӹ�Ԫ��-------------------------���
											// e2
				nodeList.add(nodeReduce); // �ڵ㼯����Ӹ�Ԫ��--------------------------���
											// n3

				// reduce��������ı� e3
				Edge egReducetUp = new Edge();
				egReducetUp.preNode = nodeShuffle;
				egReducetUp.nextNode = nodeReduce;
				egReducetUp.edgeWeight = rt.reduceSize;// ���о�����ֵ���ļ���С

				// n3���e3
				nodeReduce.preEdges.add(egReducetUp);
				// ����ڵ�ı߼��� �ۼ� n4���e3
				nodeShuffle.nextEdges.add(egReducetUp);

				// �㼯�Ϻͱ߼��Ϸֱ������Ԫ��
				edgeList.add(egReducetUp); // �߼�����Ӹ�Ԫ��--------------------------���
											// e3

			}
			nodeList.add(nodeShuffle); // �ڵ㼯����Ӹ�Ԫ��----------------------------���
										// n4

			// n6 �����job��ҵ�Ŀ�ʼ�ڵ�
			Node nodeJobStart = new Node();
			nodeJobStart.nextEdges = new ArrayList<Edge>();// ������
			nodeJobStart.preEdges = new ArrayList<Edge>();// һ����
			nodeJobStart.nodeWeight = 0.0;// ����ڵ�ȨֵΪ0
			nodeJobStart.rankU = 0.0;
			nodeJobStart.taskType = 6;
			// nodeJobStart.taskSize=0;

			for (MapTask mt : jb.mapTask) {
				map_LSumTime += (mt.mapRunTime + mt.mapSize / fd);// ---------------------------------�����ٶ�ѡ��
				// n5
				Node nodeMap = new Node();
				nodeMap.preEdges = new ArrayList<Edge>();// һ����
				nodeMap.nextEdges = new ArrayList<Edge>();// һ����
				nodeMap.nodeWeight = mt.mapRunTime;// map������ʱ��
				nodeMap.rankU = 0.0;
				nodeMap.taskType = 5;// map����
				nodeMap.taskSize = mt.mapSize;

				// shuffle�ڵ�����ıߣ�weight=0
				// e4
				Edge egMapDown = new Edge();
				egMapDown.preNode = nodeMap;
				egMapDown.nextNode = nodeShuffle;
				egMapDown.edgeWeight = 0.0;

				// n5���n4
				List<Edge> tmpEgMapDownList = new ArrayList<Edge>();
				tmpEgMapDownList.add(egMapDown);
				nodeMap.nextEdges = tmpEgMapDownList;

				// ����ڵ� n4���e4
				nodeShuffle.preEdges.add(egMapDown);

				// �㼯�Ϻͱ߼��Ϸֱ������Ԫ��
				edgeList.add(egMapDown); // �߼�����Ӹ�Ԫ��---------------------------���
											// e4
				nodeList.add(nodeMap); // �ڵ㼯����Ӹ�Ԫ��----------------------------���
										// n5

				// e5 map����ڵ��������
				Edge egMapUp = new Edge();
				egMapUp.preNode = nodeJobStart;
				egMapUp.nextNode = nodeMap;
				egMapUp.edgeWeight = mt.mapSize;// ���ļ������С

				// ������ҵ��ʼ��ӱ߼� n6���e5
				nodeJobStart.nextEdges.add(egMapUp);

				// �㼯�Ϻͱ߼��Ϸֱ������Ԫ��
				edgeList.add(egMapUp); // �߼�����Ӹ�Ԫ��-----------------------------���
										// e5
				// ÿ����ҵ��map��reduce�׶�������ʱ�����
				jb.L_map_sumTime = map_LSumTime;
				jb.L_reduce_sumTime = reduce_LSumTime;
			}
			nodeList.add(nodeJobStart); // �ڵ㼯����Ӹ�Ԫ��---------------------------���
										// n6
			// e6
			Edge egJobStart = new Edge();
			egJobStart.preNode = tail;
			egJobStart.nextNode = nodeJobStart;
			egJobStart.edgeWeight = 0.0;// �����

			tail.nextEdges.add(egJobStart);// ��Ӻ���ı߼���

			// �㼯�Ϻͱ߼��Ϸֱ������Ԫ��
			edgeList.add(egJobStart); // �߼�����Ӹ�Ԫ��------------------------------���
										// e6
		}
		// �㼯�Ϻͱ߼��Ϸֱ������Ԫ��
		nodeList.add(tail); // �ڵ㼯����Ӹ�Ԫ��------------------------��� n7
		// ��֤
		// System.out.println("node size:" + nodeList.size());
		// System.out.println("edge size:" + edgeList.size());
	}
	
	public static double taskToScheduler_DAG(int nodeNum,int mapSlotNum,char EA_EF){
		// -----------------------�������нڵ��rankU--------------------------------
		// �ڵ��rankU(i)=w(i)+max(c+rankU(j)) �����ƽ��ʱ��+ǰ����·�����ʱ��
		// c������ʱ��  ,
		calcuRankU();
		//------------------------����node����rankU��ֵ����---------------------------
		sortRankU(nodeList);// ��������
		Collections.reverse(nodeList);// reverse ����

		// ��������node���� ����map reduce|slot : currentMapSlotTime mapSlotSpeed
		// ��ʼ��map reduce slot (int nodeNum, int mapSlotNum)
		Map tmpMap = SourceScheduler.initSlots(nodeNum, mapSlotNum);
		List<MapSlot> listMapSlot = (List) tmpMap.get("listMapSlot");
		List<ReduceSlot> listReduceSlot = (List) tmpMap.get("listReduceSlot");
		
		// -----------------����״̬��ÿ����ҵmap�������������ʱ�丳ֵ��reduce slot-------------
		// ��������ҵ��map�������ʱ�� ������������Ϊreduce slot�Ŀ�ʼʱ��
		// ��������ҵ������ʱ��������� ��С����
		// SourceScheduler.jobSortAsMapLBTime(listJobs);
		// for(Job jb:listJobs){
		// System.out.println("job L_sumtime:"+jb.L_map_sumTime);
		// }
		// System.out.println("job size:"+listJobs.size());//50����ҵ
		// System.out.println("-----------------------------------");

		// ��ҵ����Զ����reduce slot����Ŀ ,����״̬��ÿ����ҵmap�������������ʱ�丳ֵ��reduce slot
		// for(int i=0;i<listReduceSlot.size();i++){
		// listReduceSlot.get(i).currentReduceSlotTime=listJobs.get(i).L_map_sumTime;
		// //System.out.println("job
		// L_sumtime:"+listReduceSlot.get(i).currentReduceSlotTime);
		// }

		//-------------------map �������map slot--------------------
		for (Node node : nodeList) {
			// map ���� ��������һ����reduceǰ�� ����
			if (node.taskType == 5) {
				// ���������ʱ��
				double runtime = node.nodeWeight;
				// ȡ��ǰʱ����С��slot �������ʱ�� �������slot���±�
				Integer	indexMinSlot ;
				if(EA_EF=='A'){
					indexMinSlot = SourceScheduler.minMapSlotTime_EA_slot_index(listMapSlot);
				}else{//EA_EF=='F'
					MapTask maptask=new MapTask();
					maptask.mapRunTime=(int) runtime;
					maptask.mapSize=node.taskSize;
	//				System.out.println("task size:"+node.taskSize);
					indexMinSlot = SourceScheduler.minMapSlotTime_EF_slot_index(listMapSlot, maptask);
				}
				
				// ��ǰ��slot ��������ʱ����� �����µ���������ʱ���׼��ʱ��
				listMapSlot.get(indexMinSlot).currentMapSlotTime += (int) (runtime + (node.taskSize / listMapSlot.get(indexMinSlot).mapSlotSpeed));

				// �õ�n4
				// ����shuffle��ʱ�䣬shuffle�ڵ��һ���ֶΣ���¼map�������ʱ��
				// ȡshuffle��ǰʱ��͵�ǰmap slot�ϵ�ǰ���ֵ��Ϊshuffle���ʱ��
				node.nextEdges.get(0).nextNode.stageTime += (runtime + (node.taskSize / listMapSlot.get(indexMinSlot).mapSlotSpeed));
				if (listMapSlot.get(indexMinSlot).currentMapSlotTime > node.nextEdges.get(0).nextNode.stageTime) {
					node.nextEdges.get(0).nextNode.stageTime = listMapSlot.get(indexMinSlot).currentMapSlotTime;
				}
			}
		}

		//----------------------��ʼ��reduce slot currentTime-----------------
		//��shuffltʱ�����򣬰�ǰreduce slot������ʱ���С��ֵ��reduce slot currentTime
		List<Double> ShuffleSum = new ArrayList<Double>();
		for (Node node : nodeList) {
			// shuffle rankU   �������ʱ��
			if (node.taskType == 4) {
				double shuffVlaue = node.stageTime;// sumMapTaskTimeShuffle(node);
				ShuffleSum.add(shuffVlaue);
			}
		}
		// System.out.println("node shuffle size:"+nodeShuffleList.size());��ҵ��
		
		// ����nodeShuffleList����С���󣬰�ֵ��ֵ��reduce slot shuffle�ڵ�Ҫ�ۼ�����map�����֮��
		sortShuffle(ShuffleSum);
		// ��������
		for (int i = 0; i < Math.min(listReduceSlot.size(),ShuffleSum.size()); i++) {
			listReduceSlot.get(i).currentReduceSlotTime = ShuffleSum.get(i);//Integer.valueOf(ShuffleSum.get(i).intValue());
		}

		
		//--------------------reduce�������reduce slot-----------------
		// shuffle�ڵ��stageTimeҪ��������������ʱ�䣬����һ��������ҵ��ɵ�ʱ��
		for (Node node : nodeList) {
			// reduce slot����֮ǰ���ܴ�0��ʼ��Ҫ���Ǳ���Ҫ��һ����ҵ������map������ɲ��ܿ�ʼ
			// ����shuffle�ڵ����һ���ߵ��¸��ڵ��ҵ����и���ҵ��reduce ���� ������ڵ�n2��stagetime
			if (node.taskType == 4) {// n4
				// n6��stageTime��ʼֵΪn4��stageTime
				node.nextEdges.get(0).nextNode.nextEdges.get(0).nextNode.stageTime = node.stageTime;
				for (Edge eg : node.nextEdges) {
					// �ñߵ���һ���ڵ����reduce ����
					double runtime = eg.nextNode.nodeWeight;// ���������ʱ��
					// indexMinSlot�������slot���±� ȡ��ǰʱ����С��slot �������ʱ��
					Integer indexMinSlot;
					if(EA_EF=='A'){
						indexMinSlot = SourceScheduler.minReduceSlotTime_EA_slot_index(listReduceSlot);
					}else{//EA_EF=='F'
						ReduceTask rt=new ReduceTask();
						rt.reduceRunTime=(int) runtime;
						rt.reduceSize=eg.nextNode.taskSize;
		//				System.out.println("reduce size:"+rt.reduceSize);
						indexMinSlot = SourceScheduler.minReduceSlotTime_EF_slot_index(listReduceSlot, rt);
					}
				
					double setupTime = eg.nextNode.taskSize / listReduceSlot.get(indexMinSlot).reduceSlotSpeed;
					// ��ǰ��slot ��������ʱ����� �����µ���������ʱ���׼��ʱ��
					listReduceSlot.get(indexMinSlot).currentReduceSlotTime += (int) (runtime + setupTime);
					// System.out.println("listReduceSlot time:" +
					// listReduceSlot.get(indexMinSlot).currentReduceSlotTime);
					// ���½ڵ�2 ����ҵ������stageTime ��ǰshuffle�ڵ��stageTime+�������ʱ��
					eg.nextNode.nextEdges.get(0).nextNode.stageTime += (runtime + setupTime);
					if (listReduceSlot.get(
							indexMinSlot).currentReduceSlotTime > eg.nextNode.nextEdges.get(0).nextNode.stageTime) {
						eg.nextNode.nextEdges.get(0).nextNode.stageTime = listReduceSlot
								.get(indexMinSlot).currentReduceSlotTime;
					}
				}
			}
		}
//		for (int i = 0; i < listReduceSlot.size(); i++) {
////			System.out.println("currentReduceSlotTime2:" + listReduceSlot.get(i).currentReduceSlotTime);
//		}

		// ȡ��node 2�����
		double maxStageTime = 0;
		for (Node node : nodeList) {
			if (node.taskType == 2) {
				if (node.stageTime > maxStageTime) {
					maxStageTime = node.stageTime;
				}
			}
		}
	////	System.out.println("maxStageTime:" + maxStageTime);
		// �õ���ǰʱ������reduce slot
//		Integer maxReduceSlotCurrentTimeIndex = SourceScheduler.maxReduceSlotValue(listReduceSlot);
//		int maxTime = listReduceSlot.get(maxReduceSlotCurrentTimeIndex).currentReduceSlotTime;
//		System.out.println("maxReduceSlotTime:" + maxTime);

		return maxStageTime;
	}
	

	// �ݹ����rankU�Ƚ��鷳���򵥵㣬���÷ֲ��㷨
	public static void calcuRankU() {
		int count = 0;
		// ��������node����ÿ���node���ݲ㼶����rankU // reduce ����� 3
		for (Node node : nodeList) {
			if (node.taskType == 1 || node.taskType == 2 || node.taskType == 3) {

				node.rankU = node.nodeWeight;
			}
		}
		// System.out.println("1-2-3 size:");

		for (Node node : nodeList) {
			// shuffle�㣬Ҫѡ�·��
			if (node.taskType == 4) {
				node.stageTime = 0;
				node.rankU = node.nodeWeight + maxRankU(node);
			}
		}
		// System.out.println("4 size:"+count);//50
		for (Node node : nodeList) {
			// map �����
			if (node.taskType == 5) {
				node.rankU = node.nodeWeight + node.nextEdges.get(0).nextNode.rankU;
			}
		}

		for (Node node : nodeList) {
			// job start
			if (node.taskType == 6) {
				node.rankU = node.nodeWeight + maxRankU(node);// 4,6һ��
			}
		}
		// System.out.println("6 size:"+count);// 50��ȷ
		for (Node node : nodeList) {
			if (node.taskType == 7) {
				node.rankU = maxRankU(node);// 4,6һ��
			}
		}
		// double rankU=0.0;
		// //���ݺ���ı߼���ȷ������·�����õ�·��rankU
		// //����漰һ�αߵļ���
		// for(Edge eg:nd.nextEdges){
		// double avgC=(eg.edgeWeight/fd+eg.edgeWeight/fr+eg.edgeWeight/fn)/3;
		// eg.nextNode.rankU= avgC+calcuRankU(eg.nextNode);
		// }
		//
		//
		// return rankU;
	}

	// rankU��������
	public static void sortRankU(List<Node> nodeList) {
		Collections.sort(nodeList, new Comparator<Node>() {// ����
			public int compare(Node nd1, Node nd2) {
				Integer nd1Rank = (int) nd1.getRankU();
				Integer nd2Rank = (int) nd2.getRankU();
				return nd1Rank.compareTo(nd2Rank);
			}
		});
	}

	// shuffle �ڵ�������ʱ���С��������
	public static void sortShuffle(List<Double> ShuffleSum) {
		Collections.sort(ShuffleSum, new Comparator<Double>() {// ����
			public int compare(Double nd1, Double nd2) {
				return nd1.compareTo(nd2);
			}
		});
	}

	// ���ھ۴����ѡ���ֵ taskType=4 ��6
	public static double maxRankU(Node nd) {
		double maxRankU = 0;
		// System.out.println("next edge size:" + nd.nextEdges.size());
		for (Edge eg : nd.nextEdges) {
			double avgC = (eg.edgeWeight / fd + eg.edgeWeight / fr + eg.edgeWeight / fn) / 3;
			double pathRankU = avgC + eg.nextNode.rankU;
			if (pathRankU > maxRankU) {
				maxRankU = pathRankU;
			}
		}
		return maxRankU;
	}

	// ����ÿ��shuffle�ڵ������������ʱ�� taskType=4
	public static double sumMapTaskTimeShuffle(Node nd) {
		double shuffleNodeValue = 0;
		for (Edge eg : nd.nextEdges) {
			double avgSetupTime = (eg.edgeWeight / fd + eg.edgeWeight / fr + eg.edgeWeight / fn) / 3;
			double runTime = eg.nextNode.nodeWeight;
			shuffleNodeValue += (avgSetupTime + runTime);
		}
		return shuffleNodeValue;
	}
}
