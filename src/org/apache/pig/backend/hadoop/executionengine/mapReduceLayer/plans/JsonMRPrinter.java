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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.io.PrintStream;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.NativeMapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PlanSerializer;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;

/*Kariz B*/
import org.json.simple.JSONObject;
import org.json.simple.*;
//import org.json.JSONException;
import java.util.UUID;
/*Kariz E*/


/**
 * A visitor mechanism printing out the logical plan.
 */
public class JsonMRPrinter extends MROpPlanVisitor {

    private PrintStream mStream = null;
    private JSONObject jObject = null;
    private boolean isVerbose = true;
    private JSONArray nodes_jarr;
    private JSONArray edges_jarr;

    /**
     * @param ps PrintStream to output plan information to
     * @param plan MR plan to print
     */
    public JsonMRPrinter(JSONObject jObject, MROperPlan plan) {
        super(plan, new DependencyOrderWalker<MapReduceOper, MROperPlan>(plan, true));
        this.nodes_jarr = new JSONArray();
	this.edges_jarr = new JSONArray();

        this.jObject = jObject;
        try {
            this.jObject.put("nodes", nodes_jarr);
	    this.jObject.put("edges", edges_jarr);
        } catch(Exception e) {
            e.printStackTrace();
        }
        	
    }

    public void setVerbose(boolean verbose) {
        isVerbose = verbose;
    }

    @Override
    public void visitMROp(MapReduceOper mr) throws VisitorException {
	JSONObject opJsonObj= new JSONObject();
        opJsonObj.put("operator",  mr.getOperatorKey().toString());
        opJsonObj.put("feature", mr.getFeature());

        if(mr instanceof NativeMapReduceOper) {
            //mStream.println(((NativeMapReduceOper)mr).getCommandString());
            //mStream.println("--------");
            //mStream.println();
            return;
        }
        if (mr.mapPlan != null && mr.mapPlan.size() > 0) {
            PlanSerializer<PhysicalOperator, PhysicalPlan> serializer = new PlanSerializer<PhysicalOperator, PhysicalPlan>(mr.mapPlan);
            opJsonObj.put("inputs", serializer.visitRoots());
	    serializer.setVerbose(isVerbose);
            opJsonObj.put("mapOps", serializer.depthFirstPP());
        }
        if (mr.combinePlan != null && mr.combinePlan.size() > 0) {
            PlanSerializer<PhysicalOperator, PhysicalPlan> serializer = new PlanSerializer<PhysicalOperator, PhysicalPlan>(mr.combinePlan);
            serializer.setVerbose(isVerbose);
	    opJsonObj.put("combinerOps", serializer.depthFirstPP());
        }
        if (mr.reducePlan != null && mr.reducePlan.size() > 0) {
            PlanSerializer<PhysicalOperator, PhysicalPlan> serializer = new PlanSerializer<PhysicalOperator, PhysicalPlan>(mr.reducePlan);
            serializer.setVerbose(isVerbose);
	    opJsonObj.put("outputs", serializer.visitLeaves());
	    opJsonObj.put("reduceOps", serializer.depthFirstPP());

        }
        if (mr.getQuantFile() != null) {
	    opJsonObj.put("QuantileFile", mr.getQuantFile());
        }
        if (mr.getUseSecondaryKey()) {
	    opJsonObj.put("SecondarySort", mr.getUseSecondaryKey());
	}
        
	this.nodes_jarr.add(opJsonObj);
    }
}

