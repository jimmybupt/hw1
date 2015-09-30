// Copyright 2013 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/nested-loop-join-node.h"

#include <sstream>

#include "codegen/llvm-codegen.h"
#include "exprs/expr.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"

#include "gen-cpp/PlanNodes_types.h"
#include <cstdio>

using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;

/**
 * Constructor
 */
NestedLoopJoinNode::NestedLoopJoinNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : BlockingJoinNode("NestedLoopJoinNode", TJoinOp::INNER_JOIN, pool, tnode, descs),right_child_pool_(new ObjectPool){
  LOG(INFO) << "constructing NestedLoopJoinNode";
}

Status NestedLoopJoinNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(BlockingJoinNode::Init(tnode));
  DCHECK(tnode.__isset.nested_loop_join_node);
  RETURN_IF_ERROR(
		  Expr::CreateExprTrees(pool_, tnode.conjuncts,
					&join_conjunct_ctxs_));

  return Status::OK;
}


/**
 * Call parent's Prepare() before doing any other processing.
 * Initialize any row pools or batches for the right child here 
 */
Status NestedLoopJoinNode::Prepare(RuntimeState* state) {
  BlockingJoinNode::Prepare(state);
  RowDescriptor full_row_desc(child(0)->row_desc(), child(1)->row_desc());
  Expr::Prepare(join_conjunct_ctxs_, state, full_row_desc, expr_mem_tracker());
  LOG(INFO) << "preparing NestedLoopJoinNode";
  return Status::OK;
}


void NestedLoopJoinNode::Close(RuntimeState* state) {
  
//right_child_batches_.Reset();
  if(is_closed())return;
  Expr::Close(join_conjunct_ctxs_, state);
  delete (right_child_pool_);
  //right_child_pool_.get()->~ObjectPool();
  BlockingJoinNode::Close(state);
  LOG(INFO) << "Return from Close()";
//right_child_pool_.get()->~ObjectPool();
}

/**
 *
* * Process each row from the left child, until eos_ is true (i.e. end of stream)
 */
Status NestedLoopJoinNode::GetNext(RuntimeState* state, RowBatch* output_batch, bool* eos) {
  LOG(INFO) << "GetNext() Called";
  if (ReachedLimit() || eos_) {
    *eos = true;
    return Status::OK;
  }

  while (!eos_) {
    // Compute max rows that should be added to output_batch
    int64_t row_batch_capacity = GetRowBatchCapacity(output_batch);

    // Continue processing this row batch
    // Call our nested loop join method here
    num_rows_returned_ +=
        DoNestedLoopJoin(output_batch, probe_batch_.get(), row_batch_capacity);

    // Sets up the internal counters required by Impala's execution engine    
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);

    if (ReachedLimit() || output_batch->AtCapacity()) {
      *eos = ReachedLimit();
      break;
    }

    // Check to see if we're done processing the current left child batch
    if (current_right_child_row_.AtEnd() && probe_batch_pos_ == probe_batch_->num_rows()) {
      probe_batch_->TransferResourceOwnership(output_batch);
      probe_batch_pos_ = 0;
      if (output_batch->AtCapacity()) break;
      if (probe_side_eos_) {
        *eos = eos_ = true;
        break;
      } else {
        child(0)->GetNext(state, probe_batch_.get(), &probe_side_eos_);
	current_right_child_row_ = right_child_batches_.Iterator();
        COUNTER_ADD(probe_row_counter_, probe_batch_->num_rows());
	if (probe_batch_->num_rows()==0) {
	  *eos = eos_ = true;
	  probe_batch_->TransferResourceOwnership(output_batch);
	  break;
	}
	current_probe_row_ = probe_batch_->GetRow(probe_batch_pos_++);
      }
    }
  }

  return Status::OK;
}

/**
 * 
 * turn: Number of rows that qualified the join conditions
 */
int NestedLoopJoinNode::DoNestedLoopJoin(RowBatch* output_batch, RowBatch* batch,
    int row_batch_capacity) {

  //Number of rows returned by this function
  int rows_returned = 0;

  ExprContext* const* join_conjunct_ctxs = &join_conjunct_ctxs_[0];
  size_t num_join_ctxs = join_conjunct_ctxs_.size();

  while (row_batch_capacity > 0){
    //LOG(INFO) << "probe batch has " << probe_batch_->num_rows() << " rows";
    TupleRow* output_row = output_batch->GetRow(output_batch->AddRow());
    //LOG(INFO) << "we have " <<num_join_ctxs << " conjunct";
    //LOG(INFO) << "right getrow";
    CreateOutputRow(output_row, current_probe_row_, current_right_child_row_.GetRow());
    if(EvalConjuncts(join_conjunct_ctxs, num_join_ctxs, output_row)){
      output_batch->CommitLastRow();
      row_batch_capacity --;
      rows_returned ++;
    }
    current_right_child_row_.Next();
    if (current_right_child_row_.AtEnd()){
      if (probe_batch_pos_ ==  probe_batch_->num_rows()) break;
      //LOG(INFO) << "left getrow";
      current_probe_row_ = probe_batch_->GetRow(probe_batch_pos_++);
      current_right_child_row_ = right_child_batches_.Iterator();
    }
  }

  return rows_returned;
}

/**
 * DO NOT MODIFY THIS.
 */
int64_t NestedLoopJoinNode::GetRowBatchCapacity(RowBatch* output_batch){
  int64_t max_added_rows = output_batch->capacity() - output_batch->num_rows();
  if (limit() != -1)
    max_added_rows = min(max_added_rows, limit() - rows_returned());
  return max_added_rows;
}

/**
 * DO NOT MODIFY THIS.
 *
 * Do a full scan of the right child [child(1)] and store all row batches
 * in right_child_batches_
 */
Status NestedLoopJoinNode::ConstructBuildSide(RuntimeState* state) {
  RETURN_IF_ERROR(child(1)->Open(state));
  while (true) {
    RowBatch* batch = right_child_pool_->Add(
        new RowBatch(child(1)->row_desc(), state->batch_size(), mem_tracker()));
    bool eos;
    child(1)->GetNext(state, batch, &eos);
    DCHECK_EQ(batch->num_io_buffers(), 0) << "Build batch should be compact.";
    SCOPED_TIMER(build_timer_);
    right_child_batches_.AddRowBatch(batch);
    VLOG_ROW << BuildListDebugString();
    COUNTER_SET(build_row_counter_,
        static_cast<int64_t>(right_child_batches_.total_num_rows()));
    if (eos) break;
  }
  return Status::OK;
}

/**
 * DO NOT MODIFY THIS.
 */
Status NestedLoopJoinNode::InitGetNext(TupleRow* first_left_row) {
  current_right_child_row_ = right_child_batches_.Iterator();
  return Status::OK;
}

string NestedLoopJoinNode::BuildListDebugString() {
  stringstream out;
  out << "BuildList(";
  out << right_child_batches_.DebugString(child(1)->row_desc());
  out << ")";
  return out.str();
}

