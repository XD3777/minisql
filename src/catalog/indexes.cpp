#include "catalog/indexes.h"


// IndexMetadata 类构造函数，初始化索引元数据
IndexMetadata::IndexMetadata(const index_id_t index_id, const std::string &index_name, const table_id_t table_id,
                             const std::vector<uint32_t> &key_map)
    : index_id_(index_id), index_name_(index_name), table_id_(table_id), key_map_(key_map) {}

// 创建新的 IndexMetadata 实例
IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name, const table_id_t table_id,
                                     const vector<uint32_t> &key_map) {
  return new IndexMetadata(index_id, index_name, table_id, key_map);
}

// 将索引元数据序列化到缓冲区
uint32_t IndexMetadata::SerializeTo(char *buf) const {
  char *p = buf;
  uint32_t ofs = GetSerializedSize();
  ASSERT(ofs <= PAGE_SIZE, "Failed to serialize index info.");
  // 写入魔数标识
  MACH_WRITE_UINT32(buf, INDEX_METADATA_MAGIC_NUM);
  buf += 4;
  // 写入索引 ID
  MACH_WRITE_TO(index_id_t, buf, index_id_);
  buf += 4;
  // 写入索引名称
  MACH_WRITE_UINT32(buf, index_name_.length());
  buf += 4;
  MACH_WRITE_STRING(buf, index_name_);
  buf += index_name_.length();
  // 写入表 ID
  MACH_WRITE_TO(table_id_t, buf, table_id_);
  buf += 4;
  // 写入键映射数量
  MACH_WRITE_UINT32(buf, key_map_.size());
  buf += 4;
  // 写入键映射
  for (auto &col_index : key_map_) {
    MACH_WRITE_UINT32(buf, col_index);
    buf += 4;
  }
  ASSERT(buf - p == ofs, "Unexpected serialize size.");
  return ofs;
}

/**
 * Student Implement
 */
// 获取序列化后索引元数据的大小
uint32_t IndexMetadata::GetSerializedSize() const {
  return 4 + 4 + 4 + index_name_.length() + 4 + 4 + key_map_.size() * 4;
}

// 从缓冲区反序列化索引元数据
uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta) {
  char *p = buf;
  // 读取魔数标识
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == INDEX_METADATA_MAGIC_NUM, "Failed to deserialize index info.");
  // 读取索引 ID
  index_id_t index_id = MACH_READ_FROM(index_id_t, buf);
  buf += 4;
  // 读取索引名称
  uint32_t len = MACH_READ_UINT32(buf);
  buf += 4;
  std::string index_name(buf, len);
  buf += len;
  // 读取表 ID
  table_id_t table_id = MACH_READ_FROM(table_id_t, buf);
  buf += 4;
  // 读取索引键数量
  uint32_t index_key_count = MACH_READ_UINT32(buf);
  buf += 4;
  // 读取键映射
  std::vector<uint32_t> key_map;
  for (uint32_t i = 0; i < index_key_count; i++) {
    uint32_t key_index = MACH_READ_UINT32(buf);
    buf += 4;
    key_map.push_back(key_index);
  }
  // 为索引元数据分配空间
  index_meta = new IndexMetadata(index_id, index_name, table_id, key_map);
  return buf - p;
}

// 创建索引的实际实现，支持不同类型的索引
Index *IndexInfo::CreateIndex(BufferPoolManager *buffer_pool_manager, const string &index_type) {
  size_t max_size = 0;
  uint32_t column_cnt = key_schema_->GetColumns().size();
  size_t size_bitmap = (column_cnt % 8) ? column_cnt / 8 + 1 : column_cnt / 8;
  // 计算最大大小（列数 + 位图大小）
  max_size += 4 + sizeof(unsigned char) * size_bitmap;
  for (auto col : key_schema_->GetColumns()) {
    // 如果是 char 类型列，计算长度
    if(col->GetType() == TypeId::kTypeChar)
      max_size += 4;
    max_size += col->GetLength();
  }

  if (index_type == "bptree") {
    // 根据最大大小确定 B+ 树索引的合适大小
    if (max_size <= 8)
      max_size = 16;
    else if (max_size <= 24)
      max_size = 32;
    else if (max_size <= 56)
      max_size = 64;
    else if (max_size <= 120)
      max_size = 128;
    else if (max_size <= 248)
      max_size = 256;
    else {
      LOG(ERROR) << "GenericKey size is too large";
      return nullptr;
    }
  } else {
    return nullptr;
  }
  // 创建 B+ 树索引并返回
  return new BPlusTreeIndex(meta_data_->index_id_, key_schema_, max_size, buffer_pool_manager);
}