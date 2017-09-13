/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoop;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;

/**
 *
 * @author Kimo
 */
public interface Reducer {
    public abstract  HashMap<Object, Object> reducer(List<Future<Object>> file);
}
