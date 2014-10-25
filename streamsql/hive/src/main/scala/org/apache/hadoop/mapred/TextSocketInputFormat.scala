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

package org.apache.hadoop.mapred

import java.io.{BufferedReader, InputStreamReader, IOException}

import org.apache.hadoop.io.{NullWritable, Text}

class TextSocketInputFormat extends SocketInputFormat[Text] {
  class TextSocketRecordReader(split: InputSplit, conf: JobConf)
    extends SocketRecordReader[Text](split, conf) {

    private lazy val inputReader: BufferedReader = {
      assert(isSocketOpened == true)
      new BufferedReader(new InputStreamReader(inputStream))
    }

    override def next(key: NullWritable, value: Text): Boolean = try {
      if (!isSocketOpened) {
        initInputStream(split.asInstanceOf[SocketInputSplit])
      }

      val nextLine = inputReader.readLine()
      if (nextLine == null) {
        false
      } else {
        value.set(nextLine)
        true
      }
    } catch {
      case e: Exception => throw new IOException(e)
    }
  }

  override def getRecordReader(split: InputSplit, conf: JobConf, reporter: Reporter) =
    new TextSocketRecordReader(split, conf)
}
