#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  
   DiskFileMetaPage *meta_page;
   meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  // 遍历所有分区，寻找有空闲页的分区
  uint32_t extent_id;
  for (extent_id = 0; extent_id < meta_page->GetExtentNums(); extent_id++) {
      char bitmap_page[PAGE_SIZE];
      page_id_t bitmap_page_id = extent_id * (BITMAP_SIZE + 1) + 1; // 位图页的物理页号
      ReadPhysicalPage(bitmap_page_id, bitmap_page);
      BitmapPage<PAGE_SIZE> *bitmap;
      bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_page);

      uint32_t page_offset = 0;
      if (bitmap->AllocatePage(page_offset)) {
        // 更新元信息
        meta_page->extent_used_page_[extent_id]++; // 直接操作数组
        meta_page->num_allocated_pages_++;
        WritePhysicalPage(bitmap_page_id, bitmap_page);
        // 返回逻辑页号
        return extent_id * BITMAP_SIZE + page_offset;
        //LOG(INFO) << "Extend_id: " << extent_id << std::endl;
        //LOG(INFO) << "page_offset: " << page_offset << std::endl;
      }
  }
  
  if (extent_id >=MAX_VALID_PAGE_ID / BITMAP_SIZE) {
    return INVALID_PAGE_ID;
  }

  char new_bitmap_page[PAGE_SIZE] = {0};
  page_id_t bitmap_physical_page = extent_id * (BITMAP_SIZE + 1) + 1;
  //ReadPhysicalPage(bitmap_physical_page, new_bitmap_page);
  BitmapPage<PAGE_SIZE> *new_bitmap;
  new_bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(new_bitmap_page);
  uint32_t page_offset = 0;
  new_bitmap->AllocatePage(page_offset);
  WritePhysicalPage(bitmap_physical_page, new_bitmap_page);

  meta_page->extent_used_page_[extent_id] += 1;
  meta_page->num_allocated_pages_ ++;
  meta_page->num_extents_ = extent_id + 1;
  //LOG(INFO) << "Extend_id: " << extent_id << std::endl;
  //LOG(INFO) << "page_offset: " << page_offset << std::endl;
  return page_offset + extent_id * BITMAP_SIZE;
  
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
    if (logical_page_id == INVALID_PAGE_ID) {
        return;
    }

    // 计算extent_id和page_offset
    uint32_t extent_id = logical_page_id / BITMAP_SIZE;
    uint32_t page_offset = logical_page_id % BITMAP_SIZE;

    DiskFileMetaPage *meta_page;
    meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

    // 检查extent_id是否有效
    if (extent_id >= meta_page->GetExtentNums()) {
        return;
    }

    // 读取位图页
    char bitmap_page_data[PAGE_SIZE];
    page_id_t bitmap_page_id = extent_id * (BITMAP_SIZE + 1) + 1;
    ReadPhysicalPage(bitmap_page_id, bitmap_page_data);
    BitmapPage<PAGE_SIZE> *bitmap_page;
    bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_page_data);

    // 释放页
    if (bitmap_page->DeAllocatePage(page_offset)) {
        // 更新元数据
        meta_page->num_allocated_pages_--;
        meta_page->extent_used_page_[extent_id]--;
  
        // 更新位图页
        WritePhysicalPage(bitmap_page_id, bitmap_page_data);
    }
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
    if (logical_page_id == INVALID_PAGE_ID) {
        return false;
    }

    // 计算extent_id和page_offset
    uint32_t extent_id = logical_page_id / BITMAP_SIZE;
    uint32_t page_offset = logical_page_id % BITMAP_SIZE;

    DiskFileMetaPage *meta_page;
    meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

    // 检查extent_id是否有效
    //if (extent_id >= meta_page->GetExtentNums()) {
    //    return false;
    //}

    // 读取位图页
    char bitmap_page_data[PAGE_SIZE];
    page_id_t bitmap_page_id = extent_id * (BITMAP_SIZE + 1) + 1;
    ReadPhysicalPage(bitmap_page_id, bitmap_page_data);
    BitmapPage<PAGE_SIZE> *bitmap_page;
    bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_page_data);

    // 检查页是否空闲
    return bitmap_page->IsPageFree(page_offset);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return logical_page_id + 1 + 1 + logical_page_id / BITMAP_SIZE;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}