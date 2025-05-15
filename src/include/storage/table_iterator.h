#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "record/row.h"
#include "page/table_page.h"

class TableHeap;

class TableIterator {
public:
 // you may define your own constructor based on your member variables
 explicit TableIterator(TableHeap *table_heap, RowId rid, Txn *txn);

 TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  // add your own private member variables here
  TableHeap *table_heap_;        // 指向所属的堆表
  Txn *txn_;                    // 当前事务
  RowId current_rid_;           // 当前行的 RowId
  TablePage *current_page_;     // 当前数据页指针
  uint32_t current_slot_;       // 当前槽位编号
  bool is_end_;                 // 是否为结束迭代器
};

#endif  // MINISQL_TABLE_ITERATOR_H
