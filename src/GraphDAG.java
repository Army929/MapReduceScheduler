import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * 类说明
 *
 * @author:Amy
 * @version:2016年10月22日下午1:05:36
 */
public class GraphDAG {
	// 给出头节点head node
	// Node head=new Node();
	// 所有点集合
	static List<Node> nodeList ;//= new ArrayList<Node>();
	// 所有边集合
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

		// 初始化：一个开始总的节点(虚拟)->job工作节点(虚拟)->map 任务工作节点->
		// Shuffle节点(虚拟)->reduce 任务工作节点->job作业结束(虚拟)->一个总结束节点(虚拟)
		// n1
		Node head = new Node();
		head.preEdges = new ArrayList<Edge>();// List<Edge>
		head.nextEdges = null;
		head.nodeWeight = 0.0;
		head.rankU = 0.0;
		head.taskType = 1; // 只有1个
		// head.taskSize = 0;

		// n7
		Node tail = new Node();
		tail.preEdges = null;// List<Edge>
		tail.nextEdges = new ArrayList<Edge>();// List<Edge>
		tail.nodeWeight = 0.0;
		tail.rankU = 0.0;
		tail.taskType = 7;// 只有1个
		// tail.taskSize = 0;
		// 空node节点列表
		nodeList.add(head);// 节点集合添加该元素------------------------------------添点 n1
		for (Job jb : listJobs) {
			// 为计算LB，每个作业的map、reduce分别的修正总时间
			int map_LSumTime = 0;
			int reduce_LSumTime = 0;

			// n2
			Node nodeJobEnd = new Node();
			nodeJobEnd.preEdges = new ArrayList<Edge>();
			nodeJobEnd.nextEdges = new ArrayList<Edge>();// 一条边
			nodeJobEnd.nodeWeight = 0.0;// 初始化参数
			nodeJobEnd.rankU = 0.0;
			nodeJobEnd.taskType = 2;
			// nodeJobEnd.taskSize = 0;

			// e1
			Edge egJobEnd = new Edge();
			egJobEnd.preNode = nodeJobEnd;// 上一个点是作业 node
			egJobEnd.nextNode = head;// 下一个点head
			egJobEnd.edgeWeight = 0;

			// n1 添加e1
			head.preEdges.add(egJobEnd);

			// 一个元素的List n2添加e1
			List<Edge> tmpEdgeJobEnd = new ArrayList<Edge>();
			tmpEdgeJobEnd.add(egJobEnd);
			nodeJobEnd.nextEdges = tmpEdgeJobEnd;

			edgeList.add(egJobEnd);// 边集合添加该元素-----------------------------------添边
									// e1
			nodeList.add(nodeJobEnd);// 节点集合添加该元素--------------------------------添点
										// n2

			// 虚拟shufflt节点，为了reduce 任务与map任务制约关系成立 n4
			Node nodeShuffle = new Node();
			nodeShuffle.preEdges = new ArrayList<Edge>();
			nodeShuffle.nextEdges = new ArrayList<Edge>();
			nodeShuffle.nodeWeight = 0.0; // 虚拟节点权重为0
			nodeShuffle.rankU = 0.0;
			nodeShuffle.taskType = 4;
			// nodeShuffle.taskSize=0;

			// 添加上一次reduce结点集合
			for (ReduceTask rt : jb.reduceTask) {
				reduce_LSumTime += (rt.reduceRunTime + rt.reduceSize / fd);// -----------------------传输速度选择
				// n3
				Node nodeReduce = new Node();
				nodeReduce.preEdges = new ArrayList<Edge>();// 一条边
				nodeReduce.nextEdges = new ArrayList<Edge>();// 一条边
				nodeReduce.nodeWeight = rt.reduceRunTime;// 初始化参数
				nodeReduce.rankU = 0.0;
				nodeReduce.taskType = 3;
				nodeReduce.taskSize = rt.reduceSize;

				// e2
				Edge egReduceDown = new Edge();
				egReduceDown.preNode = nodeReduce;// 上一个节点连接reduce 任务
				egReduceDown.nextNode = nodeJobEnd;// 下一个点nodeJob
				egReduceDown.edgeWeight = 0.0;// 该边大小还是为0

				// 节点的下一条边 n3添加e2
				List<Edge> egReduceDownList = new ArrayList<Edge>();
				egReduceDownList.add(egReduceDown);
				nodeReduce.nextEdges = egReduceDownList;

				// 作业节点添加前条边 是一个集合，有很多条 n2节点前面有多条边集 n2添加e2
				nodeJobEnd.preEdges.add(egReduceDown);

				// 点集合和边集合分别添加新元素
				edgeList.add(egReduceDown); // 边集合添加该元素-------------------------添边
											// e2
				nodeList.add(nodeReduce); // 节点集合添加该元素--------------------------添点
											// n3

				// reduce任务上面的边 e3
				Edge egReducetUp = new Edge();
				egReducetUp.preNode = nodeShuffle;
				egReducetUp.nextNode = nodeReduce;
				egReducetUp.edgeWeight = rt.reduceSize;// 边有具体数值，文件大小

				// n3添加e3
				nodeReduce.preEdges.add(egReducetUp);
				// 虚拟节点的边集合 累计 n4添加e3
				nodeShuffle.nextEdges.add(egReducetUp);

				// 点集合和边集合分别添加新元素
				edgeList.add(egReducetUp); // 边集合添加该元素--------------------------添边
											// e3

			}
			nodeList.add(nodeShuffle); // 节点集合添加该元素----------------------------添点
										// n4

			// n6 虚拟的job作业的开始节点
			Node nodeJobStart = new Node();
			nodeJobStart.nextEdges = new ArrayList<Edge>();// 多条边
			nodeJobStart.preEdges = new ArrayList<Edge>();// 一条边
			nodeJobStart.nodeWeight = 0.0;// 虚拟节点权值为0
			nodeJobStart.rankU = 0.0;
			nodeJobStart.taskType = 6;
			// nodeJobStart.taskSize=0;

			for (MapTask mt : jb.mapTask) {
				map_LSumTime += (mt.mapRunTime + mt.mapSize / fd);// ---------------------------------传输速度选择
				// n5
				Node nodeMap = new Node();
				nodeMap.preEdges = new ArrayList<Edge>();// 一条边
				nodeMap.nextEdges = new ArrayList<Edge>();// 一条边
				nodeMap.nodeWeight = mt.mapRunTime;// map的运行时间
				nodeMap.rankU = 0.0;
				nodeMap.taskType = 5;// map任务
				nodeMap.taskSize = mt.mapSize;

				// shuffle节点上面的边，weight=0
				// e4
				Edge egMapDown = new Edge();
				egMapDown.preNode = nodeMap;
				egMapDown.nextNode = nodeShuffle;
				egMapDown.edgeWeight = 0.0;

				// n5添加n4
				List<Edge> tmpEgMapDownList = new ArrayList<Edge>();
				tmpEgMapDownList.add(egMapDown);
				nodeMap.nextEdges = tmpEgMapDownList;

				// 虚拟节点 n4添加e4
				nodeShuffle.preEdges.add(egMapDown);

				// 点集合和边集合分别添加新元素
				edgeList.add(egMapDown); // 边集合添加该元素---------------------------添边
											// e4
				nodeList.add(nodeMap); // 节点集合添加该元素----------------------------添点
										// n5

				// e5 map任务节点的上条边
				Edge egMapUp = new Edge();
				egMapUp.preNode = nodeJobStart;
				egMapUp.nextNode = nodeMap;
				egMapUp.edgeWeight = mt.mapSize;// 有文件输入大小

				// 虚拟作业开始添加边集 n6添加e5
				nodeJobStart.nextEdges.add(egMapUp);

				// 点集合和边集合分别添加新元素
				edgeList.add(egMapUp); // 边集合添加该元素-----------------------------添边
										// e5
				// 每个作业的map，reduce阶段修正总时间更新
				jb.L_map_sumTime = map_LSumTime;
				jb.L_reduce_sumTime = reduce_LSumTime;
			}
			nodeList.add(nodeJobStart); // 节点集合添加该元素---------------------------添点
										// n6
			// e6
			Edge egJobStart = new Edge();
			egJobStart.preNode = tail;
			egJobStart.nextNode = nodeJobStart;
			egJobStart.edgeWeight = 0.0;// 虚拟边

			tail.nextEdges.add(egJobStart);// 添加后面的边集合

			// 点集合和边集合分别添加新元素
			edgeList.add(egJobStart); // 边集合添加该元素------------------------------添边
										// e6
		}
		// 点集合和边集合分别添加新元素
		nodeList.add(tail); // 节点集合添加该元素------------------------添点 n7
		// 验证
		// System.out.println("node size:" + nodeList.size());
		// System.out.println("edge size:" + edgeList.size());
	}
	
	public static double taskToScheduler_DAG(int nodeNum,int mapSlotNum,char EA_EF){
		// -----------------------计算所有节点的rankU--------------------------------
		// 节点的rankU(i)=w(i)+max(c+rankU(j)) 任务的平均时间+前任务路径最大时间
		// c：传输时间  ,
		calcuRankU();
		//------------------------所有node按照rankU的值排序---------------------------
		sortRankU(nodeList);// 递增排序
		Collections.reverse(nodeList);// reverse 降序

		// 给排序后的node任务 分配map reduce|slot : currentMapSlotTime mapSlotSpeed
		// 初始化map reduce slot (int nodeNum, int mapSlotNum)
		Map tmpMap = SourceScheduler.initSlots(nodeNum, mapSlotNum);
		List<MapSlot> listMapSlot = (List) tmpMap.get("listMapSlot");
		List<ReduceSlot> listReduceSlot = (List) tmpMap.get("listReduceSlot");
		
		// -----------------理想状态把每个作业map任务的所有修正时间赋值给reduce slot-------------
		// 对所有作业的map任务完成时间 ，升序排序，作为reduce slot的开始时间
		// 对所有作业的修正时间进行排序 从小到大
		// SourceScheduler.jobSortAsMapLBTime(listJobs);
		// for(Job jb:listJobs){
		// System.out.println("job L_sumtime:"+jb.L_map_sumTime);
		// }
		// System.out.println("job size:"+listJobs.size());//50个作业
		// System.out.println("-----------------------------------");

		// 作业数永远大于reduce slot的数目 ,理想状态把每个作业map任务的所有修正时间赋值给reduce slot
		// for(int i=0;i<listReduceSlot.size();i++){
		// listReduceSlot.get(i).currentReduceSlotTime=listJobs.get(i).L_map_sumTime;
		// //System.out.println("job
		// L_sumtime:"+listReduceSlot.get(i).currentReduceSlotTime);
		// }

		//-------------------map 任务分配map slot--------------------
		for (Node node : nodeList) {
			// map 任务 在排序中一定在reduce前面 降序
			if (node.taskType == 5) {
				// 任务的运行时间
				double runtime = node.nodeWeight;
				// 取当前时间最小的slot 最早完成时间 最早可用slot的下标
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
				
				// 当前的slot 处理任务，时间更新 ，加新的任务运行时间和准备时间
				listMapSlot.get(indexMinSlot).currentMapSlotTime += (int) (runtime + (node.taskSize / listMapSlot.get(indexMinSlot).mapSlotSpeed));

				// 得到n4
				// 更新shuffle的时间，shuffle节点多一个字段，记录map任务完成时刻
				// 取shuffle当前时间和当前map slot上当前最大值作为shuffle完成时间
				node.nextEdges.get(0).nextNode.stageTime += (runtime + (node.taskSize / listMapSlot.get(indexMinSlot).mapSlotSpeed));
				if (listMapSlot.get(indexMinSlot).currentMapSlotTime > node.nextEdges.get(0).nextNode.stageTime) {
					node.nextEdges.get(0).nextNode.stageTime = listMapSlot.get(indexMinSlot).currentMapSlotTime;
				}
			}
		}

		//----------------------初始化reduce slot currentTime-----------------
		//对shufflt时间排序，把前reduce slot个数的时间大小赋值给reduce slot currentTime
		List<Double> ShuffleSum = new ArrayList<Double>();
		for (Node node : nodeList) {
			// shuffle rankU   任务完成时间
			if (node.taskType == 4) {
				double shuffVlaue = node.stageTime;// sumMapTaskTimeShuffle(node);
				ShuffleSum.add(shuffVlaue);
			}
		}
		// System.out.println("node shuffle size:"+nodeShuffleList.size());作业数
		
		// 排序nodeShuffleList，从小到大，把值赋值给reduce slot shuffle节点要累计所有map任务的之和
		sortShuffle(ShuffleSum);
		// 递增排序
		for (int i = 0; i < Math.min(listReduceSlot.size(),ShuffleSum.size()); i++) {
			listReduceSlot.get(i).currentReduceSlotTime = Integer.valueOf(ShuffleSum.get(i).intValue());
		}

		
		//--------------------reduce任务分配reduce slot-----------------
		// shuffle节点的stageTime要加所有任务的完成时间，才是一个完整作业完成的时间
		for (Node node : nodeList) {
			// reduce slot处理之前不能从0开始，要考虑必须要有一个作业的所有map任务都完成才能开始
			// 根据shuffle节点的下一个边的下个节点找到所有该作业的reduce 任务 ，计算节点n2的stagetime
			if (node.taskType == 4) {// n4
				// n6的stageTime初始值为n4的stageTime
				node.nextEdges.get(0).nextNode.nextEdges.get(0).nextNode.stageTime = node.stageTime;
				for (Edge eg : node.nextEdges) {
					// 该边的下一个节点就是reduce 任务
					double runtime = eg.nextNode.nodeWeight;// 任务的运行时间
					// indexMinSlot最早可用slot的下标 取当前时间最小的slot 最早完成时间
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
					// 当前的slot 处理任务，时间更新 ，加新的任务运行时间和准备时间
					listReduceSlot.get(indexMinSlot).currentReduceSlotTime += (int) (runtime + setupTime);
					// System.out.println("listReduceSlot time:" +
					// listReduceSlot.get(indexMinSlot).currentReduceSlotTime);
					// 更新节点2 ，作业结束的stageTime 当前shuffle节点的stageTime+任务完成时间
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

		// 取出node 2中最大
		double maxStageTime = 0;
		for (Node node : nodeList) {
			if (node.taskType == 2) {
				if (node.stageTime > maxStageTime) {
					maxStageTime = node.stageTime;
				}
			}
		}
	////	System.out.println("maxStageTime:" + maxStageTime);
		// 得到当前时间最大的reduce slot
//		Integer maxReduceSlotCurrentTimeIndex = SourceScheduler.maxReduceSlotValue(listReduceSlot);
//		int maxTime = listReduceSlot.get(maxReduceSlotCurrentTimeIndex).currentReduceSlotTime;
//		System.out.println("maxReduceSlotTime:" + maxTime);

		return maxStageTime;
	}
	

	// 递归计算rankU比较麻烦，简单点，采用分层算法
	public static void calcuRankU() {
		int count = 0;
		// 遍历所有node，给每层的node根据层级计算rankU // reduce 任务层 3
		for (Node node : nodeList) {
			if (node.taskType == 1 || node.taskType == 2 || node.taskType == 3) {

				node.rankU = node.nodeWeight;
			}
		}
		// System.out.println("1-2-3 size:");

		for (Node node : nodeList) {
			// shuffle层，要选最长路径
			if (node.taskType == 4) {
				node.stageTime = 0;
				node.rankU = node.nodeWeight + maxRankU(node);
			}
		}
		// System.out.println("4 size:"+count);//50
		for (Node node : nodeList) {
			// map 任务层
			if (node.taskType == 5) {
				node.rankU = node.nodeWeight + node.nextEdges.get(0).nextNode.rankU;
			}
		}

		for (Node node : nodeList) {
			// job start
			if (node.taskType == 6) {
				node.rankU = node.nodeWeight + maxRankU(node);// 4,6一致
			}
		}
		// System.out.println("6 size:"+count);// 50正确
		for (Node node : nodeList) {
			if (node.taskType == 7) {
				node.rankU = maxRankU(node);// 4,6一致
			}
		}
		// double rankU=0.0;
		// //根据后面的边集合确定后面路径，得到路径rankU
		// //最多涉及一次边的集合
		// for(Edge eg:nd.nextEdges){
		// double avgC=(eg.edgeWeight/fd+eg.edgeWeight/fr+eg.edgeWeight/fn)/3;
		// eg.nextNode.rankU= avgC+calcuRankU(eg.nextNode);
		// }
		//
		//
		// return rankU;
	}

	// rankU递增排序
	public static void sortRankU(List<Node> nodeList) {
		Collections.sort(nodeList, new Comparator<Node>() {// 升序
			public int compare(Node nd1, Node nd2) {
				Integer nd1Rank = (int) nd1.getRankU();
				Integer nd2Rank = (int) nd2.getRankU();
				return nd1Rank.compareTo(nd2Rank);
			}
		});
	}

	// shuffle 节点任务总时间从小到大排序
	public static void sortShuffle(List<Double> ShuffleSum) {
		Collections.sort(ShuffleSum, new Comparator<Double>() {// 升序
			public int compare(Double nd1, Double nd2) {
				return nd1.compareTo(nd2);
			}
		});
	}

	// 对于聚簇情况选最大值 taskType=4 ，6
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

	// 计算每个shuffle节点上所有任务的时间 taskType=4
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
