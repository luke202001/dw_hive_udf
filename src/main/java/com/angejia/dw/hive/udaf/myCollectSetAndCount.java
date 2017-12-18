package com.angejia.dw.hive.udaf;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/*
add jar /export/data/hiveUDF.jar;
create temporary function collect_list  as 'com.test.hive.udf.GroupConcat';

select id, collect_list(value) from test group by id;


| id   | value |
+------+-------+
|    1 | a     |
|    1 | a     |
|    1 | b     |
|    2 | c     |
|    2 | d     |
|    2 | d     |
+------+-------+

1    ["a", "2", "b", "1"]
2    ["d", "2", "c", "1"]


        */
public class myCollectSetAndCount extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1, "Exactly one argument is expected.");
        }
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted.");
        }
        return new myCollectSetAndCountEvaluator();
    }
    public static enum Mode {
        PARTIAL1, //从原始数据到部分聚合数据的过程（map阶段），将调用iterate()和terminatePartial()方法。
        PARTIAL2, //从部分聚合数据到部分聚合数据的过程（map端的combiner阶段），将调用merge() 和terminatePartial()方法。
        FINAL,    //从部分聚合数据到全部聚合的过程（reduce阶段），将调用merge()和 terminate()方法。
        COMPLETE  //从原始数据直接到全部聚合的过程（表示只有map，没有reduce，map端直接出结果），将调用merge() 和 terminate()方法。
    };

    public static class myCollectSetAndCountEvaluator extends GenericUDAFEvaluator {
        protected PrimitiveObjectInspector inputKeyOI;
        protected StandardListObjectInspector loi;
        protected StandardListObjectInspector internalMergeOI;
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            if (m == Mode.PARTIAL1) {
                inputKeyOI = (PrimitiveObjectInspector) parameters[0];
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI));
            } else {
                if ( parameters[0] instanceof StandardListObjectInspector ) {
                    internalMergeOI = (StandardListObjectInspector) parameters[0];
                    inputKeyOI = (PrimitiveObjectInspector) internalMergeOI.getListElementObjectInspector();
                    loi = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
                    return loi;
                } else {
                    inputKeyOI = (PrimitiveObjectInspector) parameters[0];
                    return ObjectInspectorFactory.getStandardListObjectInspector(
                            ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI));
                }
            }
        }

        static class MkListAggregationBuffer implements AggregationBuffer {
            List<Object> container = Lists.newArrayList();
        }
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((MkListAggregationBuffer) agg).container.clear();
        }
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MkListAggregationBuffer ret = new MkListAggregationBuffer();
            return ret;
        }
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            if(parameters == null || parameters.length != 1){
                return;
            }
            Object key = parameters[0];
            if (key != null) {
                MkListAggregationBuffer myagg = (MkListAggregationBuffer) agg;
                putIntoList(key, myagg.container);
            }
        }

        private void putIntoList(Object key, List<Object> container) {
            Object pCopy = ObjectInspectorUtils.copyToStandardObject(key,  this.inputKeyOI);
            container.add(pCopy);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg)
                throws HiveException {
            MkListAggregationBuffer myagg = (MkListAggregationBuffer) agg;
            List<Object> ret = Lists.newArrayList(myagg.container);
            return ret;
        }
        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            if(partial == null){
                return;
            }
            MkListAggregationBuffer myagg = (MkListAggregationBuffer) agg;
            List<Object> partialResult = (List<Object>) internalMergeOI.getList(partial);
            for (Object ob: partialResult) {
                putIntoList(ob, myagg.container);
            }
            return;
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MkListAggregationBuffer myagg = (MkListAggregationBuffer) agg;
            Map<Text, Integer> map = Maps.newHashMap();
            for (int i = 0; i< myagg.container.size() ; i++){
                Text key = (Text) myagg.container.get(i);
                if (map.containsKey(key)) {
                    map.put(key, map.get(key) + 1);
                }else{
                    map.put(key, 1);
                }
            }
            List<Map.Entry<Text, Integer>> listData = Lists.newArrayList(map.entrySet());
            Collections.sort(listData, new Comparator<Map.Entry<Text, Integer>>() {
                public int compare(Map.Entry<Text, Integer> o1, Map.Entry<Text, Integer> o2) {
                    if (o1.getValue() < o2.getValue())
                        return 1;
                    else if (o1.getValue() == o2.getValue())
                        return 0;
                    else
                        return -1;
                }
            });

            List<Object> ret =  Lists.newArrayList();
            for(Map.Entry<Text, Integer> entry : listData){
                ret.add(entry.getKey());
                ret.add(new Text(entry.getValue().toString()));
            }
            return ret;
        }
    }

}
