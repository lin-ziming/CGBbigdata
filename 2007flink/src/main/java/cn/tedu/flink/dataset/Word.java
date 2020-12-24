package cn.tedu.flink.dataset;

/**
 * @author Haitao
 * @date 2020/12/24 17:02
 */
public class Word {
    private String word;
    private Integer count;

    @Override
    public String toString() {
        return "Word{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
