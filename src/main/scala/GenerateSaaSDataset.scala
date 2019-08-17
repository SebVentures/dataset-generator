package com.dataintoresults.etl.example;

import scala.concurrent._

import java.util.logging.LogManager
import java.util.logging.Logger
import java.util.logging.Level

import java.time.LocalDate
import java.time.temporal.ChronoUnit
import java.time.format.DateTimeFormatter
import java.io._
import java.util.zip.GZIPOutputStream



object Conf {
  val nbCustomers = 10
}





object Customer {
  private var id = 1L
  
  def nextCustomerId() = {
    id = id + 1
    id
  }
  
  def nextCustomerName() = "Sponge Bob"
  
  val dateFormatter = DateTimeFormatter.ofPattern("YYYY-MM-dd")
}



case class Customer(id: Long = Customer.nextCustomerId, name: String, acquisitionChannel: String, persona: String,
    churnProbability: Seq[Double], mrrEvolution: Seq[Double]) {
  def toSeq: Seq[Any] = Seq(id, name, acquisitionChannel, persona)
  def toCsvRow: String = toSeq.mkString(",")
}

case class CustomerMonth(customerId: Long, date: LocalDate, mrr: Double){
  def toSeq: Seq[Any] = Seq(customerId, date, mrr)
  def toCsvRow: String = toSeq.mkString(",")
}


abstract class Distribution(distrib: Seq[Double]) {
  def distribution = distrib
  def length = distribution.length
  def get = distribution
  
  def par = distrib
  
  def normalize: Distribution = {
    normalize(1)
  }
  
  def normalize(sum: Double): Distribution = {
    val ratio = 1/distrib.sum*sum
    this * ratio
  }
  
  
  def *(d2: Distribution): Distribution = {
    if(d2.length != length)
      throw new RuntimeException("Distribution are not of the same length")
    GivenDistribution(distribution.zip(d2.distribution).map(v => v._1*v._2))
  }
  
  def +(d2: Distribution): Distribution = {
    if(d2.length != length)
      throw new RuntimeException("Distribution are not of the same length")
    GivenDistribution(distribution.zip(d2.distribution).map(v => v._1+v._2))
  }
  
  def *(mul: Double): Distribution = {
    GivenDistribution(distribution.map(v => v*mul))
  }
  
  def +(add: Double): Distribution = {
    GivenDistribution(distribution.map(v => v+add))
  } 
  
  override def toString(): String = distrib.mkString("[", ", ", "]")
}

case class GivenDistribution(distrib: Seq[Double]) extends Distribution(distrib)


case class GivenFirstNumbersDistribution(_length: Int, start: Double*) extends Distribution({
  val lastValue = start.last
  val l = 0 to _length zipAll(start, lastValue, lastValue) map { case (i, v) => v }
  l
})


case class LogDistribution(_length: Int) extends Distribution({
  for(i <- 0 to _length) yield Math.log10(2+i)
})

case class SinusDistribution(_length: Int, periodLength: Int) extends Distribution({
  val factor = (2*Math.PI) / periodLength
  for(i <- 0 to _length) yield Math.sin(i * factor)+1
})

case class LinearDistribution(_length: Int) extends Distribution({
  for(i <- 0 to _length) yield i.toDouble
})

case class RandomDistribution(_length: Int, min: Double, max: Double) extends Distribution({
  val range = max - min
  for(i <- 0 to _length) yield Math.random()*range + min
})

case class StableDistribution(_length: Int, value: Double) extends Distribution({
  for(i <- 0 to _length) yield value
})



abstract class Persona {
  def newCustomer(): Customer
}

class PersonaSME extends Persona {  
  def newCustomer() = Customer(
      name = "SME Client "+PersonaSME.getSmeId, 
      acquisitionChannel = PersonaSME.acquisitionChannel,
      persona = "SME",
      churnProbability = PersonaSME.churnProbability.get,
      mrrEvolution = PersonaSME.mrrEvolution.get)
}

class PersonaCorporate extends Persona {  
  def newCustomer() = Customer(
      name = "Corporate Client "+PersonaCorporate.getCorporateId, 
      acquisitionChannel = PersonaCorporate.acquisitionChannel,
      persona = "Corporate",
      churnProbability = PersonaCorporate.churnProbability.get,
      mrrEvolution = PersonaCorporate.mrrEvolution.get)
}

class PersonaStartup extends Persona {  
  def newCustomer() = Customer(
      name = "Startup Client "+PersonaStartup.getStartupId, 
      acquisitionChannel = PersonaStartup.acquisitionChannel,
      persona = "Startup",
      churnProbability = PersonaStartup.churnProbability.get,
      mrrEvolution = PersonaStartup.mrrEvolution.get)
}

object PersonaSME {  
  private var smeId = 0
  
  def getSmeId = { smeId = smeId + 1; smeId}
  val churnProbability = GivenFirstNumbersDistribution(100, 0.5, 0.3, 0.1, 0.2, 0.005) 
  
  def acquisitionChannel = if(Math.random() < 0.8) "Sales" else "Organic" 
  
  def mrrEvolution = (StableDistribution(100, 50.0*(Math.random()+0.5)) + LinearDistribution(100))
}

object PersonaCorporate {  
  private var corporateId = 0
  
  def getCorporateId = { corporateId = corporateId + 1; corporateId}
  val churnProbability = GivenFirstNumbersDistribution(100, 0.1, 0.1, 0.1, 0.2, 0.005, 0.005, 0.5, 0.005) 
  
  def acquisitionChannel = if(Math.random() < 0.95) "Sales" else "Organic" 
  
  def mrrEvolution = GivenFirstNumbersDistribution(100, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 500.0, 1000.0)*(Math.random()+0.5)
}

object PersonaStartup {  
  private var startupId = 0
  
  def getStartupId = { startupId = startupId + 1; startupId}
  val churnProbability = GivenFirstNumbersDistribution(100, 0.5, 0.5, 0.5, 0.1, 0.01) 
  
  def acquisitionChannel = if(Math.random() < 0.05) "Sales" else "Organic" 
  
  def mrrEvolution = (LinearDistribution(100)*5+20)*(Math.random()+0.5)
}


object GenerateSaaSDataset extends App {

  
  
  val customerCSV = new PrintWriter(new GZIPOutputStream(new FileOutputStream("customer.csv.gz")))
  customerCSV.println("id, name, acquisitionChannel, persona")
      
  val lifetimeCSV = new PrintWriter(new GZIPOutputStream(new FileOutputStream("lifetime.csv.gz")))
  customerCSV.println("customerId, date, mrr")
    
  generateDataset(Conf.nbCustomers, LocalDate.parse("2016-01-01"), LocalDate.parse("2018-12-01"))
    
  customerCSV.close()
  lifetimeCSV.close()


  def generateCustomers(startDate: LocalDate, distribution: Distribution, persona: Persona) : Unit = {    
    val duration = distribution.length
      
    0 to duration zip distribution.get foreach { case (i, nb) =>
      val cohortStartDate = startDate.plus(i, ChronoUnit.MONTHS)
      val cohortMaxLife =  duration - i
      0 to Math.round(nb.toFloat) foreach { _ => 
        val (cust, life) = generateCustomer(cohortStartDate, cohortMaxLife, persona)
        customerCSV.println(cust.toCsvRow)
        life.foreach(l => lifetimeCSV.println(l.toCsvRow))
      }
    }
  }
    
  def generateCustomer(startDate: LocalDate, maxDuration : Int, persona: Persona): (Customer, Seq[CustomerMonth]) = {  
     
    val customer = persona.newCustomer()
    
    val churnProbability = customer.churnProbability
    val mrrDistribution = customer.mrrEvolution    
       
      
    var alive = true
    val life = for(i <- 0 until maxDuration if alive) yield {
      val date = if(i == 0) startDate else startDate.plus(i, ChronoUnit.MONTHS)
      val l = 
      if(Math.random() < churnProbability(i)) 
        alive = false 
        
      CustomerMonth(customer.id, date, mrrDistribution(i))
    }
      
    (customer, life)
  }
    
    
  def generateDataset(nbCustomers: Int, startDate: LocalDate, endDate: LocalDate) = {
      
    val duration = ChronoUnit.MONTHS.between(startDate, endDate).toInt
      
            
    generateCustomers(startDate, LogDistribution(duration).normalize(nbCustomers*4/20), new PersonaSME)
      
    generateCustomers(startDate, LinearDistribution(duration).normalize(nbCustomers*1/20), new PersonaCorporate)
      
    generateCustomers(startDate, LinearDistribution(duration).normalize(nbCustomers*15/20), new PersonaStartup)
     
    
  }  
}

