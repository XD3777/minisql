#include "catalog/catalog.h"

// 将 CatalogMeta 元数据序列化到内存缓冲区中
void CatalogMeta::SerializeTo(char *buffer) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buffer, CATALOG_METADATA_MAGIC_NUM); // 写入魔数标识
  buffer += sizeof(uint32_t);

  MACH_WRITE_UINT32(buffer, table_meta_pages_.size()); // 写入表数量
  buffer += sizeof(uint32_t);

  MACH_WRITE_UINT32(buffer, index_meta_pages_.size()); // 写入索引数量
  buffer += sizeof(uint32_t);

  // 写入每个表的元数据信息
  for (const auto &entry : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buffer, entry.first);
    buffer += sizeof(table_id_t);
    MACH_WRITE_TO(page_id_t, buffer, entry.second);
    buffer += sizeof(page_id_t);
  }

  // 写入每个索引的元数据信息
  for (const auto &entry : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buffer, entry.first);
    buffer += sizeof(index_id_t);
    MACH_WRITE_TO(page_id_t, buffer, entry.second);
    buffer += sizeof(page_id_t);
  }
}

// 从内存缓冲区中反序列化 CatalogMeta 元数据
CatalogMeta *CatalogMeta::DeserializeFrom(char *buffer) {
  uint32_t magic = MACH_READ_UINT32(buffer);
  buffer += sizeof(uint32_t);
  ASSERT(magic == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");

  // 读取表和索引数量
  uint32_t table_count = MACH_READ_UINT32(buffer);
  buffer += sizeof(uint32_t);
  uint32_t index_count = MACH_READ_UINT32(buffer);
  buffer += sizeof(uint32_t);

  // 创建 CatalogMeta 对象并填充内容
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_count; ++i) {
    table_id_t tid = MACH_READ_FROM(table_id_t, buffer);
    buffer += sizeof(table_id_t);
    page_id_t pid = MACH_READ_FROM(page_id_t, buffer);
    buffer += sizeof(page_id_t);
    meta->table_meta_pages_.emplace(tid, pid);
  }
  for (uint32_t i = 0; i < index_count; ++i) {
    index_id_t iid = MACH_READ_FROM(index_id_t, buffer);
    buffer += sizeof(index_id_t);
    page_id_t pid = MACH_READ_FROM(page_id_t, buffer);
    buffer += sizeof(page_id_t);
    meta->index_meta_pages_.emplace(iid, pid);
  }
  return meta;
}
/**
 * Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  return sizeof(uint32_t) * 3 +
         table_meta_pages_.size() * (sizeof(table_id_t) + sizeof(page_id_t)) +
         index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t));
}

CatalogMeta::CatalogMeta() {}

/**
 * @return a new instance of CatalogMeta
 */
// CatalogManager 构造函数：加载或初始化 CatalogMeta 元数据
CatalogManager::CatalogManager(BufferPoolManager *bpm, LockManager *lm, LogManager *logm, bool init)
    : buffer_pool_manager_(bpm), lock_manager_(lm), log_manager_(logm) {
  if (init) {
    catalog_meta_ = CatalogMeta::NewInstance();
  } else {
    // 从磁盘加载 CatalogMeta
    auto meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(reinterpret_cast<char *>(meta_page->GetData()));
    next_index_id_ = catalog_meta_->GetNextIndexId();
    next_table_id_ = catalog_meta_->GetNextTableId();

    // 加载所有表和索引
    for (const auto &tbl : catalog_meta_->table_meta_pages_) {
      if (tbl.second == INVALID_PAGE_ID) break;
      LoadTable(tbl.first, tbl.second);
    }
    for (const auto &idx : catalog_meta_->index_meta_pages_) {
      if (idx.second == INVALID_PAGE_ID) break;
      LoadIndex(idx.first, idx.second);
    }
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  }
}

// 析构函数：释放 CatalogMeta 及所有表和索引
CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto &tbl : tables_) delete tbl.second;
  for (auto &idx : indexes_) delete idx.second;
}

/**
 * @param table_name the name of the table stored in the table_names_ map
 * @param schema the schema of the table
 * @param txn the transaction that is creating the table
 * @param table_info the table info that is created
 * @return DB_SUCCESS if the table is created successfully, DB_ALREADY_EXIST if the table already exists
 * @brief Create a table with the given name and schema
 */
// 创建表并注册到 Catalog 中
dberr_t CatalogManager::CreateTable(const string &name, TableSchema *schema, Txn *txn, TableInfo *&info) {
  if (table_names_.count(name) > 0) return DB_TABLE_ALREADY_EXIST;

  page_id_t meta_pid, heap_pid;
  auto meta_page = buffer_pool_manager_->NewPage(meta_pid);
  TablePage *heap_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(heap_pid));
  heap_page->Init(heap_pid, INVALID_PAGE_ID, log_manager_, txn);

  table_id_t tid = next_table_id_;
  catalog_meta_->table_meta_pages_[tid] = meta_pid;
  next_table_id_ = catalog_meta_->GetNextTableId();

  auto deep_schema = TableSchema::DeepCopySchema(schema);
  auto heap = TableHeap::Create(buffer_pool_manager_, heap_pid, deep_schema, log_manager_, lock_manager_);
  auto meta = TableMetadata::Create(tid, name, heap_pid, deep_schema);
  meta->SerializeTo(meta_page->GetData());

  buffer_pool_manager_->UnpinPage(meta_pid, true);
  buffer_pool_manager_->UnpinPage(heap_pid, true);

  info = TableInfo::Create();
  info->Init(meta, heap);
  table_names_[name] = tid;
  tables_[tid] = info;
  FlushCatalogMetaPage();

  return DB_SUCCESS;
}

/**
 * @param table_name the name of the table stored in the table_names_ map
 * @param table_info the table info that is returned
 * @return DB_SUCCESS if the table is found, DB_TABLE_NOT_EXIST if the table does not exist
 * @brief Get a table
 */
// 获取表并返回 TableInfo
dberr_t CatalogManager::GetTable(const string &name, TableInfo *&info) {
  auto iter = table_names_.find(name);
  if (iter == table_names_.end()) return DB_TABLE_NOT_EXIST;
  info = tables_.at(iter->second);
  return DB_SUCCESS;
}

/**
 * @param tables the vector of table info that is returned
 * @return DB_SUCCESS if the tables are found
 * @brief Get all tables
 */
// 获取所有表的信息
dberr_t CatalogManager::GetTables(vector<TableInfo *> &table_list) const {
  table_list.clear();
  for (const auto &entry : tables_) table_list.push_back(entry.second);
  return DB_SUCCESS;
}

/**
 * @param table_name the name of the table stored in the table_names_ map
 * @param index_name the name of the index stored in the index_names_ map
 * @param index_keys the keys of the index that is created
 * @param txn the transaction that is creating the index
 * @param index_info the index info that is created
 * @param index_type the type of the index that is created
 * @return DB_TABLE_NOT_EXIST if the table does not exist, DB_INDEX_ALREADY_EXIST if the index already exists, DB_COLUMN_NAME_NOT_EXIST if the column name does not exist in the schema, DB_SUCCESS if the index is created successfully
 * @brief Create an index on a table
 */
// 创建索引并将其添加到 Catalog 中
dberr_t CatalogManager::CreateIndex(const string &tbl_name, const string &idx_name,
                                    const vector<string> &key_cols, Txn *txn, IndexInfo *&info,
                                    const string &type) {
  auto tbl_it = table_names_.find(tbl_name);
  if (tbl_it == table_names_.end()) return DB_TABLE_NOT_EXIST;

  auto idx_it = index_names_.find(tbl_name);
  if (idx_it != index_names_.end() && idx_it->second.count(idx_name) > 0) return DB_INDEX_ALREADY_EXIST;

  page_id_t meta_pid;
  auto meta_page = buffer_pool_manager_->NewPage(meta_pid);
  next_index_id_ = catalog_meta_->GetNextIndexId();

  auto tbl_info = tables_.at(tbl_it->second);
  auto schema = tbl_info->GetSchema();

  vector<uint32_t> key_map;
  for (const auto &col : key_cols) {
    uint32_t idx;
    if (schema->GetColumnIndex(col, idx) == DB_COLUMN_NAME_NOT_EXIST) return DB_COLUMN_NAME_NOT_EXIST;
    key_map.push_back(idx);
  }

  index_id_t iid = next_index_id_;
  index_names_[tbl_name][idx_name] = iid;
  catalog_meta_->index_meta_pages_[iid] = meta_pid;

  auto meta = IndexMetadata::Create(iid, idx_name, tbl_it->second, key_map);
  meta->SerializeTo(meta_page->GetData());
  buffer_pool_manager_->UnpinPage(meta_pid, true);

  info = IndexInfo::Create();
  info->Init(meta, tbl_info, buffer_pool_manager_);

  auto heap = tbl_info->GetTableHeap();
  vector<Field> fields;
  for (auto iter = heap->Begin(nullptr); iter != heap->End(); ++iter) {
    fields.clear();
    for (auto k : key_map) fields.push_back(*(iter->GetField(k)));
    Row row(fields);
    info->GetIndex()->InsertEntry(row, iter->GetRowId(), txn);
  }

  indexes_[iid] = info;
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * @param table_name the name of the table stored in the table_names_ map
 * @param index_name the name of the index stored in the index_names_ map
 * @param index_info the index info that is returned
 * @return DB_TABLE_NOT_EXIST if the table does not exist, DB_INDEX_NOT_FOUND if the index does not exist, DB_SUCCESS if the index is found
 * @brief Get an index of a table
 */
// 获取索引信息
dberr_t CatalogManager::GetIndex(const string &tbl_name, const string &idx_name, IndexInfo *&info) const {
  auto idx_grp = index_names_.find(tbl_name);
  if (idx_grp == index_names_.end()) return DB_TABLE_NOT_EXIST;

  auto idx_it = idx_grp->second.find(idx_name);
  if (idx_it == idx_grp->second.end()) return DB_INDEX_NOT_FOUND;

  info = indexes_.at(idx_it->second);
  return DB_SUCCESS;
}

/**
 * @param table_name the name of the table stored in the table_names_ map
 * @param indexes the vector of index info that is returned
 * @return DB_INDEX_NOT_FOUND if the table does not exist, DB_SUCCESS if the indexes are found
 * @brief Get all indexes of a table
 */

// 获取表的所有索引
dberr_t CatalogManager::GetTableIndexes(const string &tbl_name, vector<IndexInfo *> &idx_list) const {
  auto idx_grp = index_names_.find(tbl_name);
  if (idx_grp == index_names_.end()) return DB_INDEX_NOT_FOUND;

  idx_list.clear();
  for (const auto &entry : idx_grp->second) idx_list.push_back(indexes_.at(entry.second));
  return DB_SUCCESS;
}

/**
 * @param table_name the name of the table stored in the table_names_ map
 * @return DB_TABLE_NOT_EXIST if the table does not exist, DB_SUCCESS if the table is dropped
 * @brief Drop a table
 */
// 删除表及其所有索引
dberr_t CatalogManager::DropTable(const string &name) {
  auto it = table_names_.find(name);
  if (it == table_names_.end()) return DB_TABLE_NOT_EXIST;

  vector<IndexInfo *> idx_list;
  if (GetTableIndexes(name, idx_list) == DB_SUCCESS) {
    for (auto &idx : idx_list) DropIndex(name, idx->GetIndexName());
  }

  table_id_t tid = it->second;
  table_names_.erase(it);

  auto tbl_info = tables_.at(tid);
  tables_.erase(tid);
  page_id_t pid = catalog_meta_->table_meta_pages_[tid];
  catalog_meta_->table_meta_pages_.erase(tid);

  tbl_info->GetTableHeap()->FreeTableHeap();
  buffer_pool_manager_->UnpinPage(pid, false);
  buffer_pool_manager_->DeletePage(pid);

  FlushCatalogMetaPage();
  delete tbl_info;
  return DB_SUCCESS;
}

/**
 * @param table_name the name of the table stored in the table_names_ map
 * @param index_name the name of the index stored in the index_names_ map
 * @return DB_TABLE_NOT_EXIST if the table does not exist, DB_INDEX_NOT_FOUND if the index does not exist, DB_SUCCESS if the index is dropped
 * @brief Drop an index of a table
 */
// 删除索引
dberr_t CatalogManager::DropIndex(const string &tbl_name, const string &idx_name) {
  auto idx_grp = index_names_.find(tbl_name);
  if (idx_grp == index_names_.end()) return DB_TABLE_NOT_EXIST;

  auto idx_it = idx_grp->second.find(idx_name);
  if (idx_it == idx_grp->second.end()) return DB_INDEX_NOT_FOUND;

  index_id_t iid = idx_it->second;
  index_names_[tbl_name].erase(idx_name);

  auto info = indexes_.at(iid);
  indexes_.erase(iid);

  page_id_t pid = catalog_meta_->index_meta_pages_[iid];
  catalog_meta_->index_meta_pages_.erase(iid);

  info->GetIndex()->Destroy();
  buffer_pool_manager_->UnpinPage(pid, false);
  buffer_pool_manager_->DeletePage(pid);

  FlushCatalogMetaPage();
  delete info;
  return DB_SUCCESS;
}

/**
 * @return DB_SUCCESS if the catalog metadata page is flushed
 * @brief Flush the catalog metadata page
 */
// 刷新 CatalogMeta 页面
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  auto page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

/**
 * @param table_id the id of the table
 * @param page_id the id of the page
 * @return DB_TABLE_ALREADY_EXIST if the table already exists, DB_SUCCESS if the table is loaded
 * @brief Load a table
 */
// 加载表的元数据
dberr_t CatalogManager::LoadTable(const table_id_t tid, const page_id_t pid) {
  if (tables_.count(tid)) return DB_TABLE_ALREADY_EXIST;

  catalog_meta_->table_meta_pages_[tid] = pid;
  auto page = buffer_pool_manager_->FetchPage(pid);

  TableMetadata *meta;
  TableMetadata::DeserializeFrom(page->GetData(), meta);

  auto schema = TableSchema::DeepCopySchema(meta->GetSchema());
  auto heap = TableHeap::Create(buffer_pool_manager_, meta->GetFirstPageId(), schema, log_manager_, lock_manager_);

  auto info = TableInfo::Create();
  info->Init(meta, heap);
  table_names_[meta->GetTableName()] = tid;
  tables_[tid] = info;

  buffer_pool_manager_->UnpinPage(pid, false);
  return DB_SUCCESS;
}

/**
 * @param index_id the id of the index
 * @param page_id the id of the page
 * @return DB_INDEX_ALREADY_EXIST if the index already exists, DB_SUCCESS if the index is loaded
 * @brief Load an index
 */
// 加载索引的元数据
dberr_t CatalogManager::LoadIndex(const index_id_t iid, const page_id_t pid) {
  if (indexes_.count(iid)) return DB_INDEX_ALREADY_EXIST;

  catalog_meta_->index_meta_pages_[iid] = pid;
  auto page = buffer_pool_manager_->FetchPage(pid);

  IndexMetadata *meta;
  IndexMetadata::DeserializeFrom(page->GetData(), meta);

  auto tbl_info = tables_.at(meta->GetTableId());
  auto info = IndexInfo::Create();
  info->Init(meta, tbl_info, buffer_pool_manager_);

  index_names_[tbl_info->GetTableName()][meta->GetIndexName()] = iid;
  indexes_[iid] = info;
  buffer_pool_manager_->UnpinPage(pid, false);
  return DB_SUCCESS;
}

/**
 * @param table_id the id of the table
 * @return DB_TABLE_NOT_EXIST if the table does not exist, DB_SUCCESS if the table is dropped
 * @brief Get a table
 */
// 获取指定表的 TableInfo
dberr_t CatalogManager::GetTable(const table_id_t tid, TableInfo *&info) {
  auto it = tables_.find(tid);
  if (it == tables_.end()) return DB_NOT_EXIST;
  info = it->second;
  return DB_SUCCESS;
}