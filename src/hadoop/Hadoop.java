/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Kimo
 */
public class Hadoop {

    public class MapCountry implements Mapper, Callable<Object> {

        HashMap<Object, Object> file = new HashMap<Object, Object>();

        @Override
        public HashMap<Object, Object> mapper(HashMap<Object, Object> file) {
            BufferedReader br = new BufferedReader((Reader) file.entrySet().iterator().next().getValue());
            String line = "";
            String cvsSplitBy = ",";
            HashMap<Object, Object> result = new HashMap<Object, Object>();

            try {
                while ((line = br.readLine()) != null) {

                    // use comma as separator
                    String[] country = line.split(cvsSplitBy);

                    if (result.containsKey(country[0].toString())) {
                        result.put(country[0].toString(), Long.valueOf((Long) result.get(country[0]) + Long.valueOf(Long.parseLong((country[1])))));
                    } else {
                        result.put(country[0], Long.valueOf(Long.parseLong(country[1])));
                    }

                }

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            return result;
//            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Object call() throws Exception {
            return this.mapper(this.file);
//            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

    }

    public class ReduceCountry implements Reducer {

        List<Future<Object>> file;

        @Override
        public HashMap<Object, Object> reducer(List<Future<Object>> file) {
            HashMap<Object, Object> result = new HashMap<Object, Object>();
            HashMap<String, Long> temp = new HashMap<String, Long>();
            for (Future<Object> fut : file) {
                try {
                    temp = (HashMap<String, Long>) fut.get();
                    for (Map.Entry<String, Long> entrySet : temp.entrySet()) {
                        String key = entrySet.getKey();
                        Long value = entrySet.getValue();
                        if (result.containsKey(key)) {
                            result.put(key, Long.valueOf((Long) result.get(key) + value));
                        } else {
                            result.put(key, Long.valueOf(value));
                        }
                    }
//                    if(result.containsKey();
//                    result.put(HashMap<Object, Object>fut.get(), Long.valueOf((long) result.get(fut.get()) + 1);
                } catch (InterruptedException ex) {
                    Logger.getLogger(Hadoop.class.getName()).log(Level.SEVERE, null, ex);
                } catch (ExecutionException ex) {
                    Logger.getLogger(Hadoop.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            return result;
//            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws FileNotFoundException, IOException, Exception {
        // TODO code application logic here
        FileReader big_data_file = new FileReader(new File("data1.txt"));
        LineNumberReader lnr = new LineNumberReader(big_data_file);
        lnr.skip(Long.MAX_VALUE);
        Long file_size = new Long(0);
        file_size += lnr.getLineNumber() + 1; //Add 1 because line index starts at 0
        // Finally, the LineNumberReader object should be closed to prevent resource leak
        lnr.close();
        int processors = Runtime.getRuntime().availableProcessors();

        double line_iteration = (double) file_size / processors;
//        line_iteration = line_iteration.doubleVal;
        line_iteration = Math.ceil(line_iteration);
        System.out.println(line_iteration);
        List<Future<Object>> map_result = new ArrayList<Future<Object>>();
        Hadoop hadoop = new Hadoop();
        ExecutorService executor = null;
        for (int i = 0; i < processors; i++) {
            MapCountry map_country = hadoop.new MapCountry();
//            map_country.
//            map_country.file = new HashMap<Double, FileReader>();
//            HashMap<Long,FileReader>file = new HashMap<Long,FileReader>();
//            file.put();
            map_country.file.put(Long.valueOf((long) line_iteration), new FileReader(new File("data"+(i+1)+".txt")));
//            map_country.call();
            //Get ExecutorService from Executors utility class, thread pool size is 10
            executor = Executors.newCachedThreadPool();

            Future<Object> future = executor.submit(map_country);
            map_result.add(future);

        }
        ReduceCountry reduce_country = hadoop.new ReduceCountry();
        reduce_country.file = map_result;
        HashMap<Object, Object> reduce_result = reduce_country.reducer(reduce_country.file);
        for (Map.Entry<Object, Object> entrySet : reduce_result.entrySet()) {
            Object key = entrySet.getKey();
            Object value = entrySet.getValue();
            System.out.println("Country " + String.valueOf(key) + " Count " + Long.valueOf((Long) value));
        }
        executor.shutdown();
        return;

    }
}
