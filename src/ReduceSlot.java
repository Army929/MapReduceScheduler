/**
 *类说明
 *
 *@author:Amy
 *@version:2016年9月1日下午3:43:04
*/
public class ReduceSlot {
  //每台节点reduce数量2
	public Integer currentReduceSlotTime;

	//reduce slot速度
	public Integer reduceSlotSpeed;
	
	public Integer getReduceSlotSpeed() {
		return reduceSlotSpeed;
	}
	public void setReduceSlotSpeed(Integer reduceSlotSpeed) {
		this.reduceSlotSpeed = reduceSlotSpeed;
	}
	public Integer getCurrentReduceSlotTime() {
		return currentReduceSlotTime;
	}
	public void setCurrentReduceSlotTime(Integer currentReduceSlotTime) {
		this.currentReduceSlotTime = currentReduceSlotTime;
	}
}
