
public class FunctionHost {
  public static <T, R> R runFunction(Function<T, R> function, T t) {
    System.out.println("pid FucntionHost: " + ProcessHandle.current().pid());
    return function.apply(t);
  }

  public static void main(String[] args) {
    Function<String, Integer> function = s -> s.length();

    System.out.println(runFunction(function, "Hello, World!"));
    // print current pid
  }
}
