#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) : size_(0), max_size_(num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock<std::mutex> lock(mutex_);

  // 如果链表为空，表示没有可替换的页面
  if (size_ == 0) {
    return false;
  }

  // 选择链表尾部的节点（最久未使用的页面）
  *frame_id = free_list_.back();

  // 从链表中移除
  free_list_.pop_back();

  // 删除节点
  free_set_.erase(*frame_id);

  // 减少当前可替换页面数
  --size_;

  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(mutex_);

  // 如果页面没有在链表中，说明它没有被 Unpin 或已经被 Pin，不需要处理
  if (free_set_.find(frame_id) == free_set_.end()) {
    return;
  }
  else {
    // 如果页面在链表中，将其移除
    free_list_.remove(frame_id);
    free_set_.erase(frame_id);

  // 减少当前可替换页面数
  --size_;
  }
  
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(mutex_);

  // 如果页面已经存在于链表中，不需要处理
  if (free_set_.find(frame_id) != free_set_.end()) {
    return;
  }
  else if (size_ >= max_size_) {
    return;
  }
  else {
    // 如果链表未满，将页面添加到链表头部
    free_list_.push_front(frame_id);
    free_set_.insert(frame_id);
    size_++;
  }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  std::scoped_lock<std::mutex> lock(mutex_);
  return size_;
}

