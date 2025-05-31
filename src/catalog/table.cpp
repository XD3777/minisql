#include "catalog/table.h"


// 将表元数据序列化到内存缓冲区
uint32_t TableMetadata::SerializeTo(char *buf) const {
  char *p = buf;
  uint32_t ofs = GetSerializedSize();
  ASSERT(ofs <= PAGE_SIZE, "Failed to serialize table info.");
  // 写入魔数标识
  MACH_WRITE_UINT32(buf, TABLE_METADATA_MAGIC_NUM);
  buf += 4;
  // 写入表 ID
  MACH_WRITE_TO(table_id_t, buf, table_id_);
  buf += 4;
  // 写入表名称
  MACH_WRITE_UINT32(buf, table_name_.length());
  buf += 4;
  MACH_WRITE_STRING(buf, table_name_);
  buf += table_name_.length();
  // 写入表的根页 ID
  MACH_WRITE_TO(page_id_t, buf, root_page_id_);
  buf += 4;
  // 写入表模式（Schema）
  buf += schema_->SerializeTo(buf);
  ASSERT(buf - p == ofs, "Unexpected serialize size.");
  return ofs;
}

/**
 * Student Implement
 */
// 计算序列化后表元数据的大小
uint32_t TableMetadata::GetSerializedSize() const {
  return 4 * 4 + table_name_.length() + schema_->GetSerializedSize();
}

/**
 *
 * @param heap Memory heap passed by TableInfo
 */
// 从缓冲区反序列化表元数据
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta) {
  
  char *p = buf;
  // 读取魔数标识
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == TABLE_METADATA_MAGIC_NUM, "Failed to deserialize table info.");
  // 读取表 ID
  table_id_t table_id = MACH_READ_FROM(table_id_t, buf);
  buf += 4;
  // 读取表名称
  uint32_t len = MACH_READ_UINT32(buf);
  buf += 4;
  std::string table_name(buf, len);
  buf += len;
  // 读取表的根页 ID
  page_id_t root_page_id = MACH_READ_FROM(page_id_t, buf);
  buf += 4;
  // 读取表的模式（Schema）
  TableSchema *schema = nullptr;
  buf += TableSchema::DeserializeFrom(buf, schema);
  // 为表元数据分配空间
  table_meta = new TableMetadata(table_id, table_name, root_page_id, schema);
  return buf - p;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
// 创建一个新的 TableMetadata 实例
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name, page_id_t root_page_id,
                                     TableSchema *schema) {
  // 为表元数据分配空间
  return new TableMetadata(table_id, table_name, root_page_id, schema);
}

// 表元数据构造函数，初始化表的元数据信息
TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
    : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
