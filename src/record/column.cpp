#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t offset = 0;
  
  // 写入魔数用于验证
  MACH_WRITE_UINT32(buf + offset, COLUMN_MAGIC_NUM);
  offset += sizeof(uint32_t);
  
  // 写入类型ID
  MACH_WRITE_UINT32(buf + offset, static_cast<uint32_t>(type_));
  offset += sizeof(uint32_t);
  
  // 写入列名长度和列名
  uint32_t name_length = name_.length();
  MACH_WRITE_UINT32(buf + offset, name_length);
  offset += sizeof(uint32_t);
  memcpy(buf + offset, name_.c_str(), name_length);
  offset += name_length;
  
  // 写入长度、表索引和标志位
  MACH_WRITE_UINT32(buf + offset, len_);
  offset += sizeof(uint32_t);
  
  MACH_WRITE_UINT32(buf + offset, table_ind_);
  offset += sizeof(uint32_t);
  
  MACH_WRITE_UINT32(buf + offset, nullable_);
  offset += sizeof(uint32_t);
  
  MACH_WRITE_UINT32(buf + offset, unique_);
  offset += sizeof(uint32_t);
  
  return offset;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  uint32_t size = 0;
  
  // 魔数、类型ID
  size += sizeof(uint32_t) + sizeof(uint32_t);
  
  // 列名长度和列名
  size += sizeof(uint32_t) + name_.length();
  
  // 长度、表索引和两个标志位
  size += sizeof(uint32_t) * 2 + sizeof(uint32_t) * 2;
  
  return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  // replace with your code here
  uint32_t offset = 0;
  
  // 读取并验证魔数
  uint32_t magic_num = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  if (magic_num != COLUMN_MAGIC_NUM) {
    LOG(ERROR) << "Column magic number mismatch";
    return 0;
  }
  
  // 读取类型ID
  TypeId type = static_cast<TypeId>(MACH_READ_UINT32(buf + offset));
  offset += sizeof(uint32_t);
  
  // 读取列名长度和列名
  uint32_t name_length = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  std::string name(buf + offset, name_length);
  offset += name_length;
  
  // 读取长度、表索引和标志位
  uint32_t len = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  
  uint32_t table_ind = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  
  bool nullable = static_cast<bool>(MACH_READ_UINT32(buf + offset));
  offset += sizeof(uint32_t);
  
  bool unique = static_cast<bool>(MACH_READ_UINT32(buf + offset));
  offset += sizeof(uint32_t);
  
  // 创建新的Column对象
  if (type == TypeId::kTypeChar) {
    column = new Column(name, type, len, table_ind, nullable, unique);
  } else {
    column = new Column(name, type, table_ind, nullable, unique);
  }
  
  return offset;
}
