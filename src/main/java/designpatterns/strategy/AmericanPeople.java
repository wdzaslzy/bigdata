package designpatterns.strategy;

/**
 * @author zhongyou_li
 */
public class AmericanPeople extends People {

    public void country() {
        System.out.println("美国");
    }

    public void say() {
        System.out.println("说英语");
    }
}
