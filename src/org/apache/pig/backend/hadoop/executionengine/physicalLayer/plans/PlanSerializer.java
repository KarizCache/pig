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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.MultiQueryPackager;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCounter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODemux;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPartialAgg;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PORank;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.Packager;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;

/*Kariz B*/
//import org.json.simple.JSONObject;
import org.json.simple.*;

import java.util.UUID;
/*Kariz E*/


public class PlanSerializer<O extends Operator, P extends OperatorPlan<O>> extends
        PlanVisitor<O, P> {

    String TAB1 = "    ";

    String TABMore = "|   ";

    String LSep = "|\n|---";

    String USep = "|   |\n|   ";

    int levelCntr = -1;

    PrintStream stream = System.out;
    boolean isVerbose = true;

    JSONArray verboseOpArr;
    JSONArray opArr;

    public PlanSerializer(P plan) {
        super(plan, new DepthFirstWalker<O, P>(plan));
	verboseOpArr = new JSONArray();
	opArr = new JSONArray();
    }

    public void setVerbose(boolean verbose) {
        isVerbose = verbose;
    }

    @Override
    public void visit() throws VisitorException {
        try {
            depthFirstPP();
        } catch (IOException ioe) {
            int errCode = 2079;
            String msg = "Unexpected error while printing physical plan.";
            throw new VisitorException(msg, errCode, PigException.BUG, ioe);
        }
    }

    public void print(OutputStream printer) throws VisitorException, IOException {
        printer.write(depthFirstPP().toString().getBytes());
    }

    protected void breadthFirst() throws VisitorException {
        List<O> leaves = mPlan.getLeaves();
        Set<O> seen = new HashSet<O>();
        breadthFirst(leaves, seen);
    }

    @SuppressWarnings("unchecked")
    private void breadthFirst(Collection<O> predecessors, Set<O> seen)
            throws VisitorException {
        ++levelCntr;
        dispTabs();

        List<O> newPredecessors = new ArrayList<O>();
        for (O pred : predecessors) {
            if (seen.add(pred)) {
                List<O> predLst = mPlan.getPredecessors(pred);
                if (predLst != null)
                    newPredecessors.addAll(predLst);

                pred.visit(this);
            }
        }
        if (newPredecessors.size() > 0) {
            stream.println();
            breadthFirst(newPredecessors, seen);
        }
    }

    @SuppressWarnings("unchecked")
    public JSONArray visitRoots() {
	JSONArray inputsJArr = new JSONArray();
	List<O> roots = mPlan.getRoots();
        Collections.sort(roots);
        for (O node : roots) {
	    if(node instanceof POLoad){
	        inputsJArr.add(((POLoad)node).getLFile().getFileName());
	    }
        }
	return inputsJArr;
    }

    @SuppressWarnings("unchecked")
    public JSONArray visitLeaves() {
        JSONArray outputsJArr = new JSONArray();
        List<O> leaves = mPlan.getLeaves();
        Collections.sort(leaves);
        for (O node : leaves) {
            if(node instanceof POStore){
                outputsJArr.add(((POStore)node).getSFile().getFileName());
            }
        }
        return outputsJArr;
    }


    @SuppressWarnings("unchecked")
    public JSONObject depthFirstPP() throws VisitorException {
	JSONObject ops = new JSONObject();
        List<O> leaves = mPlan.getLeaves();
        Collections.sort(leaves);
        for (O leaf : leaves) {
            depthFirst(leaf);
        }
        ops.put("verbose", verboseOpArr);
	ops.put("ops", opArr);
	return ops;
    }

    private String planString(PhysicalPlan pp){
        StringBuilder sb = new StringBuilder();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        if(pp!=null)
            pp.explain(baos, isVerbose);
        else
            return "";
        sb.append(USep);
        sb.append(shiftStringByTabs(baos.toString(), 2));
        return sb.toString();
    }

    private String planString(List<PhysicalPlan> lep){
        StringBuilder sb = new StringBuilder();
        if(lep!=null)
            for (PhysicalPlan ep : lep) {
                sb.append(planString(ep));
            }
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private void depthFirst(O node) throws VisitorException {
        if (isVerbose) {
          if(node instanceof POFilter){
	      verboseOpArr.add("POFilter");
          }
          else if(node instanceof POLocalRearrange){
	      verboseOpArr.add("POLocalRearrange");
          }
          else if(node instanceof POPartialAgg){
	      verboseOpArr.add("POPartialAgg");
          }
          else if(node instanceof POCollectedGroup){
	      verboseOpArr.add("POCollectedGroup");
          }
          else if(node instanceof PORank){
	      verboseOpArr.add("PORank");
          }
          else if(node instanceof POCounter){
	      verboseOpArr.add("POCounter");
          }
          else if(node instanceof POSort){
              verboseOpArr.add("POSort");
          }
          else if(node instanceof POForEach){
              verboseOpArr.add("POForEach");
          }
          else if(node instanceof POPackage){
              Packager pkgr = ((POPackage) node).getPkgr();
       	      if (pkgr instanceof MultiQueryPackager) {
   	         verboseOpArr.add("MultiQueryPackager");
   	      } else {
   	         verboseOpArr.add("POPackager");
   	      }
          }
          else if(node instanceof POFRJoin){
	      verboseOpArr.add("POFRJoin");
          }
          else if(node instanceof POSkewedJoin){
	      verboseOpArr.add("POSkewedJoin");
          }
          else if(node instanceof POLimit) {
	      verboseOpArr.add("POLimit");
          }
        }

        if (node instanceof POSplit) {
	    opArr.add("POSplit");
        }
        else if (node instanceof PODemux) {
	    opArr.add("PODemux");
        }

        List<O> originalPredecessors = mPlan.getPredecessors(node);
        if (originalPredecessors == null)
            return;

        List<O> predecessors =  new ArrayList<O>(originalPredecessors);

        Collections.sort(predecessors);
        int i = 0;
        for (O pred : predecessors) {
            i++;
            depthFirst(pred);
        }
    }

    private String shiftStringByTabs(String DFStr, int TabType) {
        StringBuilder sb = new StringBuilder();
        String[] spl = DFStr.split("\n");

        String tab = (TabType == 1) ? TAB1 : TABMore;

        sb.append(spl[0] + "\n");
        for (int i = 1; i < spl.length; i++) {
            sb.append(tab);
            sb.append(spl[i]);
            sb.append("\n");
        }
        return sb.toString();
    }

    private void dispTabs() {
        for (int i = 0; i < levelCntr; i++)
            stream.print(TAB1);
    }

    public void visitLoad(POLoad op) {
        stream.print(op.name() + "   ");
    }

    public void visitStore(POStore op) {
        stream.print(op.name() + "   ");
    }

    public void visitFilter(POFilter op) {
        stream.print(op.name() + "   ");
    }

    public void visitLocalRearrange(POLocalRearrange op) {
        stream.print(op.name() + "   ");
    }

    public void visitGlobalRearrange(POGlobalRearrange op) {
        stream.print(op.name() + "   ");
    }

    public void visitPackage(POPackage op) {
        stream.print(op.name() + "   ");
    }

    public void visitStartMap(POUnion op) {
        stream.print(op.name() + "   ");
    }

}
