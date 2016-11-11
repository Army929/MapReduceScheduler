import java.util.ArrayList;
import java.util.List;

/**
 * 类说明
 *
 * @author:Amy
 * @version:2016年9月1日下午3:42:45
 */
public class MapSlot {
	// map，reudce slot数量并不一定，作为参数在变
	// 每台节点map slot数量：2，4，6，8 ，跟节点数无关
	public Double currentMapSlotTime;

	// map slot的速度
	public Integer mapSlotSpeed;

	// TBS 算法需要记录添加的mapTask的顺序,每个Slot需要记录maptask任务，只对map阶段，没有reduce阶段
	List<MapTask> maptaskSlotList = new ArrayList<MapTask>();

	public Integer getMapSlotSpeed() {
		return mapSlotSpeed;
	}

	public void setMapSlotSpeed(Integer mapSlotSpeed) {
		this.mapSlotSpeed = mapSlotSpeed;
	}

	public Double getCurrentMapSlotTime() {
		return currentMapSlotTime;
	}

	public void setCurrentMapSlotTime(Double currentMapSlotTime) {
		this.currentMapSlotTime = currentMapSlotTime;
	}

}
