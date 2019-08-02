package designpatterns.strategy;

/**
 * @author zhongyou_li
 */
public class ChinesePeople extends People {
    
    public void country() {
        System.out.println("中国");
    }

    public void say() {
        System.out.println("说汉语");
    }

}
