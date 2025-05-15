#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
    
  // 检查是否有空闲页
    if (page_allocated_ >= GetMaxSupportedSize()) {
        return false;
    }

    // 从next_free_page_开始查找空闲页
    uint32_t start_byte = next_free_page_ / 8;
    uint8_t start_bit = next_free_page_ % 8;

    // 先查找起始字节中剩余的位
    for (uint8_t bit = start_bit; bit < 8; ++bit) {
        if (IsPageFreeLow(start_byte, bit)) {
            page_offset = start_byte * 8 + bit;
            bytes[start_byte] |= (1 << bit);  // 标记为已分配
            ++page_allocated_;
            next_free_page_ = page_offset + 1;
            return true;
        }
    }

    // 查找后续字节
    for (uint32_t byte = start_byte + 1; byte < MAX_CHARS; ++byte) {
        if (bytes[byte] != 0xFF) {  // 如果字节不全为1，说明有空闲位
            for (uint8_t bit = 0; bit < 8; ++bit) {
                if (!(bytes[byte] & (1 << bit))) {
                    page_offset = byte * 8 + bit;
                    bytes[byte] |= (1 << bit);  // 标记为已分配
                    ++page_allocated_;
                    next_free_page_ = page_offset + 1;
                    return true;
                }
            }
        }
    }

    // 如果没有找到空闲页，这是不可能的情况，因为前面已经检查过page_allocated_
    LOG(ERROR) << "BitmapPage::AllocatePage: No free page found but page_allocated_ < GetMaxSupportedSize()";
    return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
    
  // 检查偏移量是否有效
    if (page_offset >= GetMaxSupportedSize()) {
        return false;
    }

    uint32_t byte_index = page_offset / 8;
    uint8_t bit_index = page_offset % 8;

    // 检查页是否已分配
    if (!IsPageFreeLow(byte_index, bit_index)) {
        bytes[byte_index] &= ~(1 << bit_index);  // 标记为空闲
        --page_allocated_;
        
        // 更新next_free_page_为当前释放的页或更小的值
        if (page_offset < next_free_page_) {
            next_free_page_ = page_offset;
        }
        return true;
    }

    return false;
    
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  // 检查偏移量是否有效
    if (page_offset >= GetMaxSupportedSize()) {
        return false;
    }

    uint32_t byte_index = page_offset / 8;
    uint8_t bit_index = page_offset % 8;

    return IsPageFreeLow(byte_index, bit_index);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  // 检查索引是否有效
    if (byte_index >= MAX_CHARS || bit_index >= 8) {
        return false;
    }

    return !(bytes[byte_index] & (1 << bit_index));
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;