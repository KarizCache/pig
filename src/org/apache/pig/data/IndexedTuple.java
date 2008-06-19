/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This is an internal class that keeps track of the specific input that a Tuple came from
 */
public class IndexedTuple extends DefaultTuple {

    /**
     * 
     */
    private static final long serialVersionUID = 2L;
    
    public int index = -1;
    
    public IndexedTuple() {
    }
    
    public IndexedTuple(Tuple t, int indexIn) {
        // Have to do it like this because Tuple is an interface, we don't
        // have access to its internal structures.
        super(t.getAll());
        index = indexIn;
    }

    @Override
    public String toString() {
        return super.toString() + "[" + index + "]";
    }

    // Writable methods:
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(index);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        index = in.readInt();
    }
    
    public Tuple toTuple(){
        return TupleFactory.getInstance().newTuple(mFields);
    }
}
