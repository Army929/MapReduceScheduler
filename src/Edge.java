/**
 *类说明：边类-数据传输
 *
 *@author:Amy
 *@version:2016年10月22日下午1:04:55
*/
public class Edge {
     //与该边相连的两个节点preNod，nextNode
	Node preNode;
	Node nextNode;	
	//传输时间->setup time
	double edgeWeight;
	
	public Node getPreNode() {
		return preNode;
	}
	public void setPreNode(Node preNode) {
		this.preNode = preNode;
	}
	public Node getNextNode() {
		return nextNode;
	}
	public void setNextNode(Node nextNode) {
		this.nextNode = nextNode;
	}
	public double getEdgeWeight() {
		return edgeWeight;
	}
	public void setEdgeWeight(double edgeWeight) {
		this.edgeWeight = edgeWeight;
	}
}
