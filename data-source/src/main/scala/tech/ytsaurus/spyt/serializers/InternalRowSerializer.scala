package tech.ytsaurus.spyt.serializers

import org.apache.spark.metrics.yt.YtMetricsRegister
import org.apache.spark.metrics.yt.YtMetricsRegister.ytMetricsSource._
import org.apache.spark.sql.catalyst.InternalRow
import tech.ytsaurus.client.TableWriter

import java.util.concurrent.{Executors, TimeUnit}
import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

object InternalRowSerializer {
  private val context = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

  final def writeRows(writer: TableWriter[InternalRow],
                      rows: java.util.ArrayList[InternalRow],
                      timeout: Duration): Future[Unit] = {
    Future {
      writeRowsRecursive(writer, rows, timeout)
    }(context)
  }

  @tailrec
  private def writeRowsRecursive(writer: TableWriter[InternalRow],
                                 rows: java.util.ArrayList[InternalRow],
                                 timeout: Duration): Unit = {
    if (!writer.write(rows)) {
      YtMetricsRegister.time(writeReadyEventTime, writeReadyEventTimeSum) {
        writer.readyEvent().get(timeout.toMillis, TimeUnit.MILLISECONDS)
      }
      writeRowsRecursive(writer, rows, timeout)
    }
  }
}


