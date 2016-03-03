package com.linkedin.camus.etl.kafka.mapred;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;

public class EtlOverwritingMultiOutputCommitter extends EtlMultiOutputCommitter {

    private Logger log;

    public EtlOverwritingMultiOutputCommitter(Path outputPath, TaskAttemptContext context, Logger log) throws IOException {
        super(outputPath, context, log);
        this.log = log;
    }

    @Override
    protected void commitFile(JobContext job, Path source, Path target) throws IOException {
        if (FileSystem.get(job.getConfiguration()).exists(target)) {
            log.info(String.format("Overwriting %s to %s", source, target));
            try {
                FileContext.getFileContext(job.getConfiguration()).rename(source, target, Options.Rename.OVERWRITE);
            } catch (Exception e) {
                log.error(String.format("Failed to overwrite from %s to %s", source, target));
                throw new IOException(String.format("Failed to overwrite from %s to %s", source, target));
            }
        } else {
            super.commitFile(job, source, target);
        }

    }

}
