#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn)
    : table_heap_(table_heap), txn_(txn), current_rid_(rid), is_end_(false) {
  if (table_heap_ == nullptr || rid.GetPageId() == INVALID_PAGE_ID) {
    is_end_ = true;
    current_page_ = nullptr;
    return;
  }

  // 获取当前页
  current_page_ = reinterpret_cast<TablePage *>(
      table_heap_->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (current_page_ == nullptr) {
    is_end_ = true;
    return;
  }

  // 初始化槽位为 RowId 对应的 slot_num
  current_slot_ = rid.GetSlotNum();
  // 确保槽位有效
  if (current_slot_ >= current_page_->GetTupleCount()) {
    is_end_ = true;
    table_heap_->buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    current_page_ = nullptr;
  }
}

TableIterator::TableIterator(const TableIterator &other)
    : table_heap_(other.table_heap_),
      txn_(other.txn_),
      current_rid_(other.current_rid_),
      current_slot_(other.current_slot_),
      is_end_(other.is_end_),
      current_page_(nullptr) {  // 显式初始化为 nullptr
  if (!is_end_) {
    // 获取当前页
    current_page_ = reinterpret_cast<TablePage *>(
        table_heap_->buffer_pool_manager_->FetchPage(current_rid_.GetPageId()));
    
    // 检查页是否有效获取
    if (current_page_ == nullptr) {
      is_end_ = true;
    } else if (current_slot_ >= current_page_->GetTupleCount()) {
      // 槽位无效，设置为 end 状态
      is_end_ = true;
      table_heap_->buffer_pool_manager_->UnpinPage(current_rid_.GetPageId(), false);
      current_page_ = nullptr;
    }
  }
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if (this == &itr) {
    return *this;
  }

  // 释放当前页
  if (current_page_ != nullptr) {
    table_heap_->buffer_pool_manager_->UnpinPage(current_rid_.GetPageId(), false);
    current_page_ = nullptr;
  }

  // 复制成员
  table_heap_ = itr.table_heap_;
  txn_ = itr.txn_;
  current_rid_ = itr.current_rid_;
  current_slot_ = itr.current_slot_;
  is_end_ = itr.is_end_;

  // 重新获取页（如果非结束状态）
  if (!is_end_) {
    current_page_ = reinterpret_cast<TablePage *>(
        table_heap_->buffer_pool_manager_->FetchPage(current_rid_.GetPageId()));
  }

  return *this;
}

TableIterator::~TableIterator() {
  if (current_page_ != nullptr) {
    table_heap_->buffer_pool_manager_->UnpinPage(current_rid_.GetPageId(), false);
    current_page_ = nullptr;
  }
}

bool TableIterator::operator==(const TableIterator &itr) const {
  if (is_end_ && itr.is_end_) {
    return true;
  }
  if (is_end_ || itr.is_end_) {
    return false;
  }
  return current_rid_ == itr.current_rid_ && table_heap_ == itr.table_heap_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  ASSERT(!is_end_, "Cannot dereference end iterator");
  static Row empty_row;
  if (current_page_->IsDeleted(current_page_->GetTupleSize(current_slot_))) {
    // 跳过已删除的行（逻辑删除场景）
    ++*this;
    return operator*();
  }

  // 获取行数据
  Row *row = new Row(current_rid_);
  current_page_->GetTuple(row, table_heap_->schema_, txn_, table_heap_->lock_manager_);
  return *row;
}

Row *TableIterator::operator->() {
  ASSERT(!is_end_, "Cannot dereference end iterator");
  if (current_page_->IsDeleted(current_page_->GetTupleSize(current_slot_))) {
    ++*this;
    return operator->();
  }

  Row *row = new Row(current_rid_);
  current_page_->GetTuple(row, table_heap_->schema_, txn_, table_heap_->lock_manager_);
  return row;
}

// ++iter
TableIterator &TableIterator::operator++() {
  if (is_end_) {
    return *this;
  }

  // 释放当前页
  table_heap_->buffer_pool_manager_->UnpinPage(current_rid_.GetPageId(), false);
  current_page_ = nullptr;

  // 查找下一个槽位
  uint32_t next_slot = current_slot_ + 1;
  page_id_t current_page_id = current_rid_.GetPageId();

  // 在当前页查找下一个有效槽位
  TablePage *page = reinterpret_cast<TablePage *>(
      table_heap_->buffer_pool_manager_->FetchPage(current_page_id));
  while (next_slot < page->GetTupleCount()) {
    if (!page->IsDeleted(page->GetTupleSize(next_slot))) {
      current_rid_ = RowId(current_page_id, next_slot);
      current_slot_ = next_slot;
      current_page_ = page;
      return *this;
    }
    next_slot++;
  }
  table_heap_->buffer_pool_manager_->UnpinPage(current_page_id, false);
  current_page_ = nullptr;

  // 当前页无更多有效行，查找下一页
  page_id_t next_page_id = page->GetNextPageId();
  while (next_page_id != INVALID_PAGE_ID) {
    TablePage *next_page = reinterpret_cast<TablePage *>(
        table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
    // 查找下一页的第一个有效槽位
    for (uint32_t slot = 0; slot < next_page->GetTupleCount(); slot++) {
      if (!next_page->IsDeleted(next_page->GetTupleSize(slot))) {
        current_rid_ = RowId(next_page_id, slot);
        current_slot_ = slot;
        current_page_ = next_page;
        table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);
        return *this;
      }
    }
    table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);
    next_page_id = next_page->GetNextPageId();
  }

  // 无更多行，设置为结束迭代器
  is_end_ = true;
  return *this;
}

// 后置递增（iter++）
TableIterator TableIterator::operator++(int) {
  TableIterator old = *this;
  ++*this;
  return old;
}
