package helpers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

public class SplunkHelper {

  private static Gson gson = new GsonBuilder().serializeNulls().serializeSpecialFloatingPointValues().create();

  public static String toString(Object in) {
    return in.getClass().getSimpleName()+render(gson.toJsonTree(in)).toString();
  }

  private static Object render(JsonElement jsonElement) {
    if (jsonElement.isJsonArray()) {
      List<Object> list = new ArrayList<>();
      jsonElement.getAsJsonArray().forEach(value->{
        list.add(render(value));
      });
      return list;
    }
    if (jsonElement.isJsonObject()) {
      Map<String, Object> hash = new LinkedHashMap<>();
      jsonElement.getAsJsonObject().entrySet().forEach(entry -> {
        hash.put(entry.getKey(), render(entry.getValue()));
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
        System.out.println(SplunkHelper.toString(new Record()));
      }
    };
  }
}
