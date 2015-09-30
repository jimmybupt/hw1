// Copyright 2012 Cloudera Inc.
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

#ifndef IMPALA_EXEC_NESTED_LOOP_JOIN_NODE_H
#define IMPALA_EXEC_NESTED_LOOP_JOIN_NODE_H

#include <boost/scoped_ptr.hpp>
#include <boost/unordered_set.hpp>
#include <boost/thread.hpp>
#include <string>

#include "exec/exec-node.h"
#include "exec/blocking-join-node.h"
#include "exec/row-batch-list.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "util/promise.h"

#include "gen-cpp/PlanNodes_types.h"

namespace impala {

class Bitmap;
class RowBatch;
class TupleRow;

/**
 * Node for Nested Loop Joins.
 * Basic idea is to build a batch pool of rows of the right child node [child(1)],
 * and then iterate over it for each of the left child rows [probe], evaluating if the conjuncts
 * (such as the join conditions) are satisfied. If it does satisfy the conjuncts,
 * write the output row into output batch, which will be pulled by the parent nodes
 * up above in the Query Plan hierarchy.
 *
 * The idea of extending this class from the BlockingJoinNode is to block this node
 * while it is waiting to build the row batches of the right child.
 *
 * Since this operator follows an iterator based model, you will some familiar functions such as
 * Open(), Prepare(), GetNext() and Close(). The first of these methods, i.e. Open(), is
 * already implemented for you in the BlockingJoinNode. It is responsible for initializing the
 * building of the right child row batches in a separate thread, so that you do not have to worry
 * about them explicitly.
 */
class NestedLoopJoinNode: public BlockingJoinNode {
public:
  NestedLoopJoinNode(ObjectPool* pool, const TPlanNode& tnode,
      const DescriptorTbl& descs);

  //Operator specific methods
  virtual Status Prepare(RuntimeState* state);
  virtual Status Init(const TPlanNode& tnode);
  // This method is partially implemented for you.
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual void Close(RuntimeState* state);

protected:
  /**
   * Initializes the build-side state (right child) for a new row
   * A NULL ptr for first_probe_row indicates the left child eos (End of Stream)
   * It is called in Open() to prepare for GetNext(), which is implemented in BlockingJoinNode.
   */
  virtual Status InitGetNext(TupleRow* first_left_row);

  /**
   * The build batches (i.e. the right child's row batches) are constructed here
   */
  virtual Status ConstructBuildSide(RuntimeState* state);

private:

  // Join conjucts
  std::vector<ExprContext*> join_conjunct_ctxs_;

  // Object pool for storing right child RowBatches
  ObjectPool * right_child_pool_;

  // List of batches of the right child, to be constructed in Prepare()
  RowBatchList right_child_batches_;

  //Pointer to the current batch in the right child.
  //Use iterator methods like GetRow() or Next() on this to iterate over
  //the rows inside this row batch. Check the row-batch-list.h file for
  //more details.
  RowBatchList::TupleRowIterator current_right_child_row_;

  /**
   *Logic for the nested loop join. For each row in the left child, iterate over all the
   *rows in the right child, evaluating whether the rows satisfy the conjuncts.
   *Qualified rows are written out to the output_batch.
   *
   *The rows from the left child can be accessed via current_probe_row_ defined in
   *BlockingJoinNode, with probe_batch_pos_ being the position referring to the current
   *row number in the batch.
   *
   *output_batch : The batch for the resulting rows
   *batch: The row batch from the left child
   *row_batch_capacity: Maximum rows that can be added to output_batch (default: 1024)
   */
  int DoNestedLoopJoin(RowBatch* output_batch, RowBatch* batch,
      int row_batch_capacity);

  /**
   * Calculates the maximum number of rows that can be added to output_batch
   */
  int64_t GetRowBatchCapacity(RowBatch* output_batch);

  /**
   * Helper method to ease debugging. By default, returns a debug string for build_rows_.
   * Feel free to append more information to this string which you think will help you
   * simplify debugging
   */
  std::string BuildListDebugString();
};

}

#endif
