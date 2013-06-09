package com.foursquare.rogue

import org.specs2.matcher.JUnitMustMatchers
import org.junit.{Test, After, Before}
import net.liftweb.mongodb.record.{BsonMetaRecord, BsonRecord, MongoRecord, MongoMetaRecord}
import net.liftweb.mongodb.record.field.{BsonRecordField, ObjectIdPk}
import net.liftweb.record.field.DateTimeField
import java.util.Calendar

/**
 * @author eiennohito
 * @since 28.02.13 
 */

class CalendarFld private() extends MongoRecord[CalendarFld] with ObjectIdPk[CalendarFld] {
  def meta = CalendarFld

  object inner extends BsonRecordField(this, CalendarInner)
}

object CalendarFld extends CalendarFld with MongoMetaRecord[CalendarFld] {
  override def mongoIdentifier = RogueTestMongo
}

class CalendarInner private() extends BsonRecord[CalendarInner] {
  def meta = CalendarInner

  object date extends DateTimeField(this) //actually calendar field, not joda DateTime
}

object CalendarInner extends CalendarInner with BsonMetaRecord[CalendarInner]

class IterateeTestForBsonRecordSubfields extends JUnitMustMatchers {
  import LiftRogue._

  @Before
  def initialize() {
    RogueTestMongo.connectToMongo

    val inner = CalendarInner.createRecord.date(Calendar.getInstance())
    CalendarFld.createRecord.inner(inner).save
  }

  @After
  def teardown() {
    CalendarFld bulkDelete_!!!()

    RogueTestMongo.disconnectFromMongo
  }

  @Test
  def testIteratee() {
    val q = CalendarFld select(_.inner.subfield(_.date))
    val cnt = q.count()
    val list = q.iterate(List[Calendar]()) {
      case (list, Iter.Item(cal)) =>
        val c: Calendar = cal.get //class cast exception was here
        c.set(Calendar.HOUR_OF_DAY, 0)
        Iter.Continue(c :: list)
      case (list, Iter.Error(e)) => e.printStackTrace(); Iter.Continue(list)
      case (list, _) => Iter.Return(list)
    }
    list.length must_== (cnt)
  }
}
