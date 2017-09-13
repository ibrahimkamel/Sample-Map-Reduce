/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoop;

import java.io.FileReader;
import java.util.HashMap;

/**
 *
 * @author Kimo
 */
public interface Mapper {

    /**
     *
     * @param File
     * @return
     */
    public abstract HashMap<Object, Object> mapper(HashMap<Object, Object> file);
            
}
