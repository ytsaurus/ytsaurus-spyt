package tech.ytsaurus.spark.log4j;

import java.util.Enumeration;

import org.apache.log4j.Appender;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.Logger;


//TODO Do we really need this class or we can get rid of it?
public class AsyncLoggerHelper extends AsyncAppender {
    public AsyncLoggerHelper() {
        super();
    }

    public void setAppenderFromLogger(String name) {
        Logger l = Logger.getLogger(name);

        Enumeration<Appender> e = l.getAllAppenders();

        while (e.hasMoreElements()) {
            addAppender(e.nextElement());
        }
    }
}
