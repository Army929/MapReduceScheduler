/**
 *��˵��������-���ݴ���
 *
 *@author:Amy
 *@version:2016��10��22������1:04:55
*/
public class Edge {
     //��ñ������������ڵ�preNod��nextNode
	Node preNode;
	Node nextNode;	
	//����ʱ��->setup time
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
