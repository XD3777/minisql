#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) { 
  // 计算行序列化后的大小
  uint32_t tuple_size = row.GetSerializedSize(schema_);
  if (tuple_size > TablePage::SIZE_MAX_ROW) {
    return false; // 行太大，无法存储
  }

  page_id_t current_page_id = first_page_id_;
  while (current_page_id != INVALID_PAGE_ID) {
    // 1. 从缓冲池获取数据页
    TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
    if (page == nullptr) {
      return false; // 页面获取失败
    }

    page->WLatch(); // 加写锁

    // 2. 尝试插入行到当前页
    bool inserted = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    if (inserted) {
      // 插入成功，记录 RowId
      RowId rid(page->GetTablePageId(), page->GetTupleCount() - 1); // 假设插入到最后一个槽位
      row.SetRowId(rid);

      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(current_page_id, true); // 标记为脏页
      return true;
    }

    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(current_page_id, false); // 未修改，无需脏页标记

    // 3. 查找下一个页（First Fit 策略）
    current_page_id = page->GetNextPageId();
  }

  // 4. 无可用页，创建新页
  TablePage *new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(current_page_id));
  if (new_page == nullptr) {
    return false; // 新页创建失败
  }

  new_page->Init(current_page_id, INVALID_PAGE_ID, log_manager_, txn); // 初始化新页

  // 链接到双向链表
  if (first_page_id_ == INVALID_PAGE_ID) {
    first_page_id_ = current_page_id; // 首页为空时设置首页
  } else {
    // 找到尾页并链接新页
    TablePage *last_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
    last_page->SetNextPageId(current_page_id);
    new_page->SetPrevPageId(last_page->GetTablePageId());
    buffer_pool_manager_->UnpinPage(last_page->GetTablePageId(), true);
  }

  // 插入行到新页
  bool inserted = new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  new_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(current_page_id, inserted); // 插入成功则标记脏页

  if (inserted) {
    RowId rid(current_page_id, 0); // 新页首个槽位
    row.SetRowId(rid);
    return true;
  }

  buffer_pool_manager_->DeletePage(current_page_id); // 插入失败，删除空页
  return false;
 }

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) { 
   // 1. 获取旧行所在页
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false;
  }

  page->WLatch();
  Row old_row;
  Row new_row = row;
  old_row.SetRowId(rid);

  // 2. 尝试在原页更新
  int update_result = page->UpdateTuple(new_row, &old_row, schema_, txn, lock_manager_, log_manager_);
  if (update_result == 1) { // 更新成功
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
  } else if (update_result == -2) { // 空间不足，需删除旧行并插入新行
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true); // 先释放旧页

    // 3. 逻辑删除旧行
    if (!MarkDelete(rid, txn)) {
      return false;
    }

    // 4. 插入新行（复用 InsertTuple 逻辑）
    bool insert_success = InsertTuple(new_row, txn);
    if (insert_success) {
      // 物理删除旧行（可选，根据回收策略）
      ApplyDelete(rid, txn);
      return true;
    }
    // 插入失败则回滚删除
    RollbackDelete(rid, txn);
    return false;
  } else { // 其他失败情况
    page->WUnlatch();
    return false;
  }
 }

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return;
  }

  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_); // 调用 TablePage 的物理删除逻辑
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true); // 标记脏页
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) { 
  RowId rid = row->GetRowId();
  if (rid.GetPageId() == INVALID_PAGE_ID) {
    return false; // 无效 RowId
  }

  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false;
  }

  page->RLatch(); // 加读锁
  bool success = page->GetTuple(row, schema_, txn, lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false); // 读操作不修改页面

  return success;
 }

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) { 
  if (first_page_id_ == INVALID_PAGE_ID) {
    return End(); // 空表返回 End 迭代器
  }

  // 获取首页的第一个有效行 RID
  TablePage *first_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  RowId first_rid;
  first_page->GetFirstTupleRid(&first_rid);
  buffer_pool_manager_->UnpinPage(first_page_id_, false); // 只读，不脏页

  return TableIterator(this, first_rid, txn);
 }

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { 
  return TableIterator(this, RowId(), nullptr); // 空 RID 表示迭代结束
 }
