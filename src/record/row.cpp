#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here

  uint32_t offset = 0;
  const uint32_t column_count = schema->GetColumnCount();
  const auto &columns = schema->GetColumns();

  // 1. 写入 Header：字段数量 + Null 位图
  MACH_WRITE_UINT32(buf + offset, column_count);
  offset += sizeof(uint32_t);

  uint32_t null_bitmap_size = (column_count + 7) / 8;
  uint8_t *null_bitmap = reinterpret_cast<uint8_t *>(buf + offset);
  memset(null_bitmap, 0, null_bitmap_size); // 初始化位图
  offset += null_bitmap_size;

  // 填充 Null 位图
  for (uint32_t i = 0; i < column_count; i++) {
    if (fields_[i]->IsNull()) {
      null_bitmap[i / 8] |= (1 << (i % 8));
    }
  }

  // 2. 序列化每个字段（直接访问联合体成员 + 类型判断）
  for (uint32_t i = 0; i < column_count; i++) {
    Field *field = fields_[i];
    if (field->IsNull()) {
      continue; // 跳过 NULL 字段
    }

    const Column *column = columns[i];
    TypeId type = column->GetType();

    switch (type) {
      case TypeId::kTypeInt: {
        // 直接访问联合体的 integer_ 成员
        int32_t val = field->GetInt();
        MACH_WRITE_UINT32(buf + offset, val);
        offset += sizeof(int32_t);
        break;
      }
      case TypeId::kTypeFloat: {
        // 直接访问联合体的 float_ 成员
        float_t val = field->GetFloat();
        memcpy(buf + offset, &val, sizeof(float_t));
        offset += sizeof(float_t);
        break;
      }
      case TypeId::kTypeChar: {
        // 通过成员函数获取字符串长度和数据
        uint32_t len = field->GetLength();
        const char *data = field->GetData();
        MACH_WRITE_UINT32(buf + offset, len);
        offset += sizeof(uint32_t);
        memcpy(buf + offset, data, len);
        offset += len;
        break;
      }
      //default:
        //throw DbException("Unsupported field type during serialization");
    }
  }

  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t offset = 0;
  const uint32_t column_count = schema->GetColumnCount();
  const auto &columns = schema->GetColumns();

  // 1. 读取 Header：字段数量 + Null 位图
  uint32_t header_column_count = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  ASSERT(header_column_count == column_count, "Column count mismatch in header");

  uint32_t null_bitmap_size = (column_count + 7) / 8;
  const uint8_t *null_bitmap = reinterpret_cast<const uint8_t *>(buf + offset);
  offset += null_bitmap_size;

  // 2. 反序列化每个字段（直接操作联合体 + 类型判断）
  for (uint32_t i = 0; i < column_count; i++) {
    const Column *column = columns[i];
    TypeId type = column->GetType();
    bool is_null = (null_bitmap[i / 8] & (1 << (i % 8))) != 0;

    Field *field = new Field(type); // 初始化字段（默认 NULL）
    field->is_null_ = is_null;

    if (!is_null) {
      switch (type) {
        case TypeId::kTypeInt: {
          // 直接设置联合体的 integer_ 成员
          int32_t val = MACH_READ_UINT32(buf + offset);
          field->value_.integer_ = val;
          field->len_ = sizeof(int32_t); // 设置长度
          field->is_null_ = false;
          offset += sizeof(int32_t);
          break;
        }
        case TypeId::kTypeFloat: {
          // 直接设置联合体的 float_ 成员
          float_t val;
          memcpy(&val, buf + offset, sizeof(float_t));
          field->value_.float_ = val;
          field->len_ = sizeof(float_t);
          field->is_null_ = false;
          offset += sizeof(float_t);
          break;
        }
        case TypeId::kTypeChar: {
          // 通过成员函数设置字符串数据
          uint32_t len = MACH_READ_UINT32(buf + offset);
          offset += sizeof(uint32_t);
          char *data = new char[len];
          memcpy(data, buf + offset, len);
          offset += len;
          // 使用 char 类型构造函数（直接操作联合体和成员变量）
          field->type_id_ = type;
          field->value_.chars_ = data;
          field->len_ = len;
          field->is_null_ = false;
          field->manage_data_ = true; // 标记为管理内存
          break;
        }
        default:
          delete field;
          //throw DbException("Unsupported field type during deserialization");
      }
    }

    fields_.push_back(field);
  }

  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t size = 0;
  const uint32_t column_count = schema->GetColumnCount();

  // Header 大小
  size += sizeof(uint32_t); // 字段数量
  size += (column_count + 7) / 8; // Null 位图

  // 各字段数据大小（直接通过成员变量判断）
  for (uint32_t i = 0; i < column_count; i++) {
    Field *field = fields_[i];
    if (field->IsNull()) {
      continue;
    }

    const Column *column = schema->GetColumn(i);
    TypeId type = column->GetType();

    switch (type) {
      case TypeId::kTypeInt:
      case TypeId::kTypeFloat:
        size += sizeof(int32_t); // 固定长度
        break;
      case TypeId::kTypeChar:
        size += sizeof(uint32_t) + field->GetLength(); // 长度 + 内容
        break;
      //default:
        //throw DbException("Unsupported field type during size calculation");
    }
  }

  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
