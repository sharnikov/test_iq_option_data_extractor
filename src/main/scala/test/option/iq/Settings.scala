package test.option.iq

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.concurrent.duration.Duration

trait Settings {
  def db(): DB
  def hdfs(): Hdfs
  def spark(): Spark
}

class AppSettings(config: Config) extends Settings {
  override def db(): DB = new DBImpl(config.as[Config]("db"))
  override def hdfs(): Hdfs = new HdfsImpl(config.as[Config]("hdfs"))
  override def spark(): Spark = new SparkImpl(config.as[Config]("spark"))
}

trait DB {
  def user(): String
  def password(): String
  def dbName(): String
  def url(): String
  def connectionProperties(): Properties
}

class DBImpl(config: Config) extends DB {
  override def user(): String = config.getString("user")
  override def password(): String = config.getString("password")
  override def dbName(): String = config.getString("db.name")
  override def url(): String = config.getString("url")

  override def connectionProperties(): Properties = {
    val connectionProperties = new Properties()
        connectionProperties.put("Driver", "org.postgresql.Driver")
        connectionProperties.put("user", user())
        connectionProperties.put("password", password())
        connectionProperties.put("url", url())
    connectionProperties
  }
}

trait Spark {
  def url(): String
  def appName(): String
}

class SparkImpl(config: Config) extends Spark {
  override def url(): String = config.getString("url")
  override def appName(): String = config.getString("app.name")
}

trait Hdfs {
  def format(): String
  def delimeter(): String
  def url(): String
}

class HdfsImpl(config: Config) extends Hdfs {
  override def format(): String = config.getString("format")
  override def delimeter(): String = config.getString("delimiter")
  override def url(): String = config.getString("url")


}

//class HdfsImpl(config: Config) extends Hdfs {
//  override def path(): Path = new Path(config.getString("path"))
//
//  override def configuration(): Configuration = {
//    val conf = new Configuration()
//    val url = config.getString("address")
//    conf.set("fs.defaultFS", url)
//    conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
//    conf.set("fs.file.impl", classOf[LocalFileSystem].getName)
//
//    conf
//  }
//}
