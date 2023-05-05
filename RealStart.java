import java.io.IOException;

public class RealStart {
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
        String[] paths01 = {"PathTo/news.csv", "OutputPath01"};
        String[] paths02 = {"OutputPath01", "OutputPath02"};
        String[] paths03 = {"PathTo/news.csv", "OutputPath03"};
        String[] paths04 = {"OutputPath02", "OutputPath04"};
        String[] paths05 = {"OutputPath04", "OutputPath05"};
        RealDataMain01.main(paths01);
        RealDataMain02.main(paths02);
        RealDataMain03.main(paths03); //Contagem de docs
        RealDataMain04.main(paths04);
        RealDataMain05.main(paths05); //Ordenação
    }
}
