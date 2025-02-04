package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.types.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.{PhysicalPlannerContext, SyntaxErrorException}
import org.grapheco.lynx.runner.ExecutionContext

case class Union(distinct: Boolean)(a: PhysicalPlan, b: PhysicalPlan, val plannerContext: PhysicalPlannerContext) extends DoublePhysicalPlan(a,b) {

  override def schema: Seq[(String, LynxType)] = left.get.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val a = this.left.get
    val b = this.right.get

    val schema1 = a.schema
    val schema2 = b.schema
    if (!schema1.toSet.equals(schema2.toSet)) throw SyntaxErrorException("All sub queries in an UNION must have the same column names")
    val record1 = a.execute(ctx).records
    val record2 = b.execute(ctx).records
    val df = DataFrame(schema, () => record1 ++ record2)
    if (distinct) df.distinct() else df
  }

}
