package cn.doitedu.dw.cn.doitedu.sparksql;

/**
 * @author charley
 * @create 2021-01-17-17
 */
public class JavaStu {
    private int id;
    private String name;
    private int age;
    private String city;
    private double scores;

    public JavaStu() {
    }

    public JavaStu(int id, String name, int age, String city, double scores) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.city = city;
        this.scores = scores;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public double getScores() {
        return scores;
    }

    public void setScores(double scores) {
        this.scores = scores;
    }

    @Override
    public String toString() {
        return "JavaStu{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", city='" + city + '\'' +
                ", scores=" + scores +
                '}';
    }
}
