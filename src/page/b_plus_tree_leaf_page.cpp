#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {//next_id不知道从哪来，--修改后，先设为无效page
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetKeySize(key_size);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {//这个返回的index不一定存在，如果超出范围说明没找到
  int left = 0;  // 0才是开始的第一个
  int right = GetSize() - 1;
  
  while (left <= right) {
    int mid = (right + left) / 2;
    int cmp = KM.CompareKeys(key, KeyAt(mid)); 
    
     if (cmp <= 0) {
      right = mid -1;//从这里出循环的话，代表mid==left,那么进一步说明 ,而在这个情况下key还大于，说明找到了就在mid，也就是left 
    } else {
      left = mid + 1;//从这里出循环的话，代表mid==right,那么进一步说明 left == right,而在这个情况下key还大于，说明没找到 ,而说明恰好在mid右边一个，就是left
    }
  }


  
  // 返回对应的index
  return left;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) { return {KeyAt(index), ValueAt(index)}; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int index = KeyIndex(key, KM); // 找到插入位置
  
  // 移动后面的元素
  for (int i = GetSize(); i > index; i--) {
    SetKeyAt(i, KeyAt(i-1));
    SetValueAt(i, ValueAt(i-1));
  }
  
  // 插入新元素
  SetKeyAt(index, key);
  SetValueAt(index, value);
  
  SetSize(GetSize()+1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int start_index = GetSize() / 2;
  int move_num = GetSize() - start_index;
  
  // 复制数据到接收页
  recipient->CopyNFrom(PairPtrAt(start_index), move_num);
  
  // 调整当前页大小
  SetSize(GetSize() - move_num);
  
  // 设置后继指针
  // 接收页的下一页应该是当前页的下一页
  recipient->SetNextPageId(GetNextPageId());
  // 当前页的下一页应该是接收页
  SetNextPageId(recipient->GetPageId());
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  int start_index = GetSize();
  
  // 复制数据
  PairCopy(PairPtrAt(start_index), src, size);
  
  
  
  SetSize(start_index+size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  int index = KeyIndex(key, KM);
  if (index < GetSize() && KM.CompareKeys(key, KeyAt(index)) == 0) {//第一个条件说明找到了最小的index,使得key<=valueAt(index)&&第二个条件说明恰好相等
    value = ValueAt(index);
    return true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int index = KeyIndex(key, KM);
  if (index >= GetSize() || KM.CompareKeys(key, KeyAt(index)) != 0) {
    return GetSize(); // key不存在
  }
  
  // 移动后面的元素
  for (int i = index; i < GetSize() - 1; i++) {
    SetKeyAt(i, KeyAt(i+1));
    SetValueAt(i, ValueAt(i+1));
  }
  
  SetSize(GetSize()-1);
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
int start_index = 0;
  int move_num = GetSize() - start_index;
  
  // 复制数据到接收页
  recipient->CopyNFrom(PairPtrAt(start_index), move_num);
  
  // 设置接收页的next_page_id为当前页的next_page_id
  recipient->SetNextPageId(GetNextPageId());
  
  // 调整当前页大小
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  recipient->CopyLastFrom(KeyAt(0), ValueAt(0));
  // 移动后面的元素填补空缺
  for (int i = 0; i < GetSize() - 1; i++) {
    SetKeyAt(i, KeyAt(i+1));
    SetValueAt(i, ValueAt(i+1));
  }
  
  SetSize(GetSize() - 1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  int start_index = GetSize();
  
  // 复制数据

  SetKeyAt(start_index, key);
    SetValueAt(start_index, value);
  
  SetSize(start_index+1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  recipient->CopyFirstFrom(KeyAt(GetSize()-1),ValueAt(GetSize()-1));
  
  SetSize(GetSize()-1);

}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  for (int i = GetSize(); i>=1; i--) {
    SetKeyAt(i, KeyAt(i-1));
    SetValueAt(i, ValueAt(i-1));
  }
  SetKeyAt(0, key);
    SetValueAt(0, value);

    SetSize(GetSize()+1);
}