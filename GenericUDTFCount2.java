package com.ailk.udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * GenericUDTFCount2 outputs the number of rows seen, twice. It's output twice
 * to test outputting of rows on close with lateral view.
 *
 */
public class GenericUDTFCount2 extends GenericUDTF {

    Integer count = Integer. valueOf(0);
    // 列记录，一个数组对应一条记录，第一个值对应第一列，第二个对应第二列，以此类推
    Object forwardObj[] = new Object[2];

    /**
     * 最后关闭，有几个map，最后就有几条记录
     */

    @Override
    public void close() throws HiveException {
        // forwardObj[0] = count;
        // // forwardObj[1] = count;
        forward( forwardObj);
        // // forward(forwardObj);
    }

    /**
     * 返回字段设置，包括列名，列类型，如下返回两列，col1和col2，都为 int类型
     */
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add( "col1");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldNames.add( "col2");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /***
     * 几个forward就有几条记录
     */

    @Override
    public void process(Object[] args) throws HiveException {
        count = Integer.valueOf( args[0].toString());
        forwardObj[0] = count ;
        forwardObj[1] = count + 1;
        forward( forwardObj);
        forward( forwardObj);
        forward( forwardObj);
    }

    public static void main(String[] args) {
        Object forwardObj[] = new Object[1];
        System. out.println(forwardObj.length );
    }

}
