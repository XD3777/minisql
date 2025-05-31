#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  std::scoped_lock<std::recursive_mutex> lock(latch_);

  // 1. Search the page table for the requested page (P).
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    // 1.1 If P exists, pin it and return it immediately.
    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->Pin(frame_id);
    return page;
  }

  // 1.2 If P does not exist, find a replacement page (R) from either the free list or the replacer.
  frame_id_t frame_id;
  if (!TryToFindFreePage(&frame_id)) {
    return nullptr;
  }

  Page *replacement_page = &pages_[frame_id];

  // 2. If R is dirty, write it back to the disk.
  if (replacement_page->IsDirty()) {
    disk_manager_->WritePage(replacement_page->GetPageId(), replacement_page->GetData());
    replacement_page->is_dirty_ = false;
  }

  // 3. Delete R from the page table and insert P.
  if (replacement_page->GetPageId() != INVALID_PAGE_ID) {
    page_table_.erase(replacement_page->GetPageId());
  }

  // Insert P into the page table
  page_table_[page_id] = frame_id;

  // 4. Update P's metadata, read in the page content from disk, and then return a pointer to P.
  replacement_page->ResetMemory();
  replacement_page->page_id_ = page_id;
  replacement_page->pin_count_ = 1;
  replacement_page->is_dirty_ = false;

  disk_manager_->ReadPage(page_id, replacement_page->GetData());

  return replacement_page;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  
  std::scoped_lock<std::recursive_mutex> lock(latch_);

  // 1. If all the pages in the buffer pool are pinned, return nullptr.
  frame_id_t frame_id;
  if (!TryToFindFreePage(&frame_id)) {
    return nullptr;
  }

  // 0. Allocate a new page
  page_id = AllocatePage();
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  Page *new_page = &pages_[frame_id];

  // 2. If R is dirty, write it back to the disk.
  if (new_page->IsDirty()) {
    disk_manager_->WritePage(new_page->GetPageId(), new_page->GetData());
  }

  // 3. Delete R from the page table and insert P.
  if (new_page->GetPageId() != INVALID_PAGE_ID) {
    page_table_.erase(new_page->GetPageId());
  }

  // Insert new page into the page table
  page_table_[page_id] = frame_id;

  // 4. Update P's metadata, zero out memory and add P to the page table.
  new_page->ResetMemory();
  memcpy(new_page->GetData(), EMPTY_PAGE_DATA, PAGE_SIZE);
  new_page->page_id_ = page_id;
  new_page->pin_count_ = 1;
  new_page->is_dirty_ = false;

  // 5. Return a pointer to P.
  return new_page;
  
 
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  std::scoped_lock<std::recursive_mutex> lock(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // 1. If P does not exist, return true.
    return true;
  }

  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];

  // 2. If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if (page->pin_count_ > 0) {
    return false;
  }

  // 3. Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  page_table_.erase(page_id);
  replacer_->Unpin(frame_id); // Remove from replacer

  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;

  free_list_.push_back(frame_id);

  // Deallocate the page
  DeallocatePage(page_id);

  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  std::scoped_lock<std::recursive_mutex> lock(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];

  //if (page->pin_count_ <= 0) {
  //  return false;
  //}
  if (page->GetPinCount() == 0) {
    return true; // 页面未被固定
  }

  page->pin_count_--;
  if (page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }

  if (is_dirty) {
    page->is_dirty_ = true;
  }

  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  std::scoped_lock<std::recursive_mutex> lock(latch_);

  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];

  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;

  return true;
}

bool BufferPoolManager::TryToFindFreePage(frame_id_t *frame_id) {
  // First, try to find a page from the free list
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }

  // If free list is empty, try to find a victim from the replacer
  if (replacer_->Size() > 0) {
    if (replacer_->Victim(frame_id)) {
      return true;
    }
  }
  //LOG(INFO) << "Free frame id: " << *frame_id << endl;
  // No available page
  return false;
}    

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  //LOG(INFO) << "Allocated page_id: " << next_page_id << endl;
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}
