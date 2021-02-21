package helpers;

import java.util.LinkedHashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

public class SplunkHelper {
  public static String render(Object in) {
    return transform(new Gson().toJsonTree(in)).toString();
  }

  private static Object transform(JsonElement jsonElement) {
    if (jsonElement.isJsonObject()) {
      Map<String, Object> hash = new LinkedHashMap<>();
      jsonElement.getAsJsonObject().entrySet().forEach(entry -> {
        hash.put(entry.getKey(), transform(entry.getValue()));
      });
      return hash;
    }
    return jsonElement;
  }

  // unit test
  public static void main(String... args) {
    new Object() { // trick for local classes
      class Record {
        class Inner {
          int baz = 3;
        }

        class Outer {
          int bar = 2;
          Inner inner = new Inner();
        }

        int foo = 1;
        Outer outer = new Outer();
      }

      {
        System.out.println(SplunkHelper.render(new Record()));
      }
    };
  }
}
