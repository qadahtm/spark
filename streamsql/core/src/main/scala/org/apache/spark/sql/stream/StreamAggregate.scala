/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.stream

import org.apache.spark.streaming.StreamingContext

import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution

case class StreamAggregate(
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: StreamPlan)(@transient ssc: StreamingContext)
  extends UnaryNode {

  lazy val sparkPlan = execution.Aggregate(partial, groupingExpressions,
    aggregateExpressions, child.sparkPlan)(ssc.sparkContext)

  override def otherCopyArgs = ssc :: Nil

  def output = aggregateExpressions.map(_.toAttribute)

  override def execute() = attachTree(this, "execute") {
    child.execute().transform(_ => sparkPlan.execute())
  }
}
