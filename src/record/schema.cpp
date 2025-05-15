#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
   uint32_t offset = 0;
  
  // 写入魔数用于验证
  MACH_WRITE_UINT32(buf + offset, SCHEMA_MAGIC_NUM);
  offset += sizeof(uint32_t);
  
  // 写入列数量
  uint32_t column_count = columns_.size();
  MACH_WRITE_UINT32(buf + offset, column_count);
  offset += sizeof(uint32_t);
  
  // 写入is_manage_标志
  MACH_WRITE_UINT32(buf + offset, is_manage_);
  offset += sizeof(uint32_t);
  
  // 依次序列化每个Column对象
  for (auto column : columns_) {
    uint32_t col_size = column->SerializeTo(buf + offset);
    offset += col_size;
  }
  
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
   uint32_t size = 0;
  
  // 魔数、列数量、is_manage_标志
  size += sizeof(uint32_t) * 2 + sizeof(uint32_t);
  
  // 所有Column对象的序列化大小
  for (auto column : columns_) {
    size += column->GetSerializedSize();
  }
  
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // replace with your code here
  uint32_t offset = 0;
  
  // 读取并验证魔数
  uint32_t magic_num = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  if (magic_num != SCHEMA_MAGIC_NUM) {
    LOG(ERROR) << "Schema magic number mismatch";
    return 0;
  }
  
  // 读取列数量
  uint32_t column_count = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  
  // 读取is_manage_标志
  bool is_manage = static_cast<bool>(MACH_READ_UINT32(buf + offset));
  offset += sizeof(uint32_t);
  
  // 反序列化每个Column对象
  std::vector<Column *> columns;
  for (uint32_t i = 0; i < column_count; i++) {
    Column *column = nullptr;
    uint32_t col_size = Column::DeserializeFrom(buf + offset, column);
    offset += col_size;
    columns.push_back(column);
  }
  
  // 创建新的Schema对象
  schema = new Schema(columns, is_manage);
  
  return offset;
}