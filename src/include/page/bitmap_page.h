#ifndef MINISQL_BITMAP_PAGE_H
#define MINISQL_BITMAP_PAGE_H

#include <bitset>

#include "common/config.h"
#include "common/macros.h"

template <size_t PageSize>
class BitmapPage {
 public:
  /**
   * @return The number of pages that the bitmap page can record, i.e. the capacity of an extent.
   */
  static constexpr size_t GetMaxSupportedSize() { return 8 * MAX_CHARS; }

  /**
   * @param page_offset Index in extent of the page allocated.
   * @return true if successfully allocate a page.
   */
  bool AllocatePage(uint32_t &page_offset);

  /**
   * @return true if successfully de-allocate a page.
   */
  bool DeAllocatePage(uint32_t page_offset);

  /**
   * @return whether a page in the extent is free
   */
  bool IsPageFree(uint32_t page_offset) const;
/*
  void SerializeTo(char* buffer) const {
    // 写入元数据
    uint32_t* uint_ptr = reinterpret_cast<uint32_t*>(buffer);
    *uint_ptr = page_allocated_;
    uint_ptr++;
    *uint_ptr = next_free_page_;
    uint_ptr++;
    
    // 写入位图数据
    unsigned char* byte_ptr = reinterpret_cast<unsigned char*>(uint_ptr);
    for (size_t i = 0; i < MAX_CHARS; i++) {
        *byte_ptr = bytes[i];
        byte_ptr++;
    }
}

void DeserializeFrom(const char* buffer) {
    // 读取元数据
    const uint32_t* uint_ptr = reinterpret_cast<const uint32_t*>(buffer);
    page_allocated_ = *uint_ptr;
    uint_ptr++;
    next_free_page_ = *uint_ptr;
    uint_ptr++;
    
    // 读取位图数据
    const unsigned char* byte_ptr = reinterpret_cast<const unsigned char*>(uint_ptr);
    for (size_t i = 0; i < MAX_CHARS; i++) {
        bytes[i] = *byte_ptr;
        byte_ptr++;
    }
}
*/
 private:
  /**
   * check a bit(byte_index, bit_index) in bytes is free(value 0).
   *
   * @param byte_index value of page_offset / 8
   * @param bit_index value of page_offset % 8
   * @return true if a bit is 0, false if 1.
   */
  bool IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const;

  /** Note: need to update if modify page structure. */
  static constexpr size_t MAX_CHARS = PageSize - 2 * sizeof(uint32_t);

 private:
  /** The space occupied by all members of the class should be equal to the PageSize */
  [[maybe_unused]] uint32_t page_allocated_;
  [[maybe_unused]] uint32_t next_free_page_;
  [[maybe_unused]] unsigned char bytes[MAX_CHARS];
};

#endif  // MINISQL_BITMAP_PAGE_H
