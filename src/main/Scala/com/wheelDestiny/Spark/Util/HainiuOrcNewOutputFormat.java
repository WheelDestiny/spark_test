package com.wheelDestiny.Spark.Util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class HainiuOrcNewOutputFormat extends
        FileOutputFormat<NullWritable, ORCUtil.HainiuOrcSerdeRow> {

    private static class OrcRecordWriter
            extends RecordWriter<NullWritable, ORCUtil.HainiuOrcSerdeRow> {
        private Writer writer = null;
        private final Path path;
        private final OrcFile.WriterOptions options;

        OrcRecordWriter(Path path, OrcFile.WriterOptions options) {
            this.path = path;
            this.options = options;
        }

        @Override
        public void write(NullWritable key, ORCUtil.HainiuOrcSerdeRow row)
                throws IOException, InterruptedException {
            if (writer == null) {
                options.inspector(row.getInspector());
                writer = OrcFile.createWriter(path, options);
            }
            writer.addRow(row.getRow());
        }

        @Override
        public void close(TaskAttemptContext context)
                throws IOException, InterruptedException {
            if (writer == null) {
                // a row with no columns
                ObjectInspector inspector = ObjectInspectorFactory.
                        getStandardStructObjectInspector(new ArrayList<String>(),
                                new ArrayList<ObjectInspector>());
                options.inspector(inspector);
                writer = OrcFile.createWriter(path, options);
            }
            writer.close();
        }
    }

    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        Path file = getDefaultWorkFile(context, "");
        return new
                HainiuOrcNewOutputFormat.OrcRecordWriter(file, OrcFile.writerOptions(
                ShimLoader.getHadoopShims().getConfiguration(context)));
    }
}
