package designpatterns.strategy;

/**
 * @author zhongyou_li
 */
public class PeopleTest {

    public static void main(String[] args) {
        People people = new ChinesePeople();
        people.setDisplayInterface(new AsianDisplayImpl());
        people.common();
        people.display();
        people.say();
    }

}
